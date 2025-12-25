"""Workflow execution engine"""

import time
from datetime import datetime, timezone
from concurrent.futures import Future, ThreadPoolExecutor, as_completed, TimeoutError
from typing import Iterator

from zahir.events import (
    JobCompletedEvent,
    JobIrrecoverableEvent,
    JobOutputEvent,
    JobPrecheckFailedEvent,
    JobRecoveryCompleted,
    JobRecoveryStarted,
    JobRecoveryTimeout,
    JobRunnableEvent,
    JobRunningEvent,
    JobStartedEvent,
    JobTimeoutEvent,
    WorkflowCompleteEvent,
    WorkflowOutputEvent,
    WorkflowStallStartEvent,
    WorkflowStallEndEvent,
    ZahirEvent,
)
from zahir.types import (
    Context,
    Job,
    ArgsType,
    DependencyType,
    JobState,
)
import uuid


def recover_workflow(
    current_job: Job[ArgsType, DependencyType],
    job_id: str,
    err: Exception,
    workflow_id: str,
    context: Context,
) -> Iterator[ZahirEvent]:
    """Attempt to recover from a failed job by invoking its recovery method

    @param current_job: The job that failed
    @param job_id: The ID of the job that failed
    @param registry: The job registry to add recovery jobs to
    @param err: The exception that caused the failure
    @param workflow_id: The ID of the workflow
    @param context: The context containing scope and registries
    """

    recovery_timeout = (
        current_job.options.recover_timeout if current_job.options else None
    )

    try:
        # hacky method to handle recovery timeouts
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(
                _run_recovery, current_job, err, context, workflow_id
            )

            try:
                recovery_start_time = datetime.now(tz=timezone.utc)
                context.job_registry.set_state(job_id, JobState.RECOVERING)
                yield JobRecoveryStarted(workflow_id, current_job, job_id)
                # Get recovery events from the future
                for event in future.result(timeout=recovery_timeout):
                    yield event
                recovery_end_time = datetime.now(tz=timezone.utc)
                recovery_duration = (
                    recovery_end_time - recovery_start_time
                ).total_seconds()
                context.job_registry.set_state(job_id, JobState.RECOVERED)
                yield JobRecoveryCompleted(
                    workflow_id, current_job, job_id, recovery_duration
                )
            except TimeoutError:
                context.job_registry.set_state(job_id, JobState.RECOVERY_TIMED_OUT)
                yield JobRecoveryTimeout(workflow_id, current_job, job_id)
    except Exception as recovery_err:
        # Oh dear! Even the recovery failed.
        context.job_registry.set_state(job_id, JobState.IRRECOVERABLE)
        yield JobIrrecoverableEvent(workflow_id, recovery_err, current_job, job_id)


def _run_recovery(
    current_job: Job, err: Exception, context: Context, workflow_id: str
) -> Iterator[ZahirEvent]:
    """Execute the recovery process for a failed job.

    @param current_job: The job that failed
    @param err: The exception that caused the failure
    @param context: The context containing scope and registries
    @param workflow_id: The ID of the workflow
    """

    for item in type(current_job).recover(
        context, current_job.input, current_job.dependencies, err
    ):
        if isinstance(item, JobOutputEvent):
            # Output event - store it and stop processing
            context.job_registry.set_output(current_job.job_id, item.output)
            item.workflow_id = workflow_id
            item.job_id = current_job.job_id
            yield item
            break
        elif isinstance(item, WorkflowOutputEvent):
            # Yield workflow output event directly
            item.workflow_id = workflow_id
            yield item
        else:
            # It's a Job - add as recovery job
            context.job_registry.add(item)


def execute_single_job(
    job_id: str,
    current_job: Job,
    context: Context,
    timing_info: dict[str, tuple[datetime, datetime]],
    workflow_id: str,
) -> Iterator[ZahirEvent]:
    """Execute a single job and handle its subjobs

    @param job_id: The ID of the job to execute
    @param current_job: The job to execute
    @param context: The context containing scope and registries
    @param timing_info: Dictionary to store (start_time, end_time) tuples by job_id
    @param workflow_id: The ID of the workflow
    """

    start_time = datetime.now(tz=timezone.utc)
    for item in type(current_job).run(
        context, current_job.input, current_job.dependencies
    ):
        if isinstance(item, JobOutputEvent):
            # Store the job output, and stop processing the iterator.

            context.job_registry.set_output(job_id, item.output)
            item.workflow_id = workflow_id
            item.job_id = job_id

            yield item
            break
        elif isinstance(item, WorkflowOutputEvent):
            # Yield workflow output event directly
            item.workflow_id = workflow_id
            yield item
        else:
            # The current job yielded a subjob; add it to the registry.

            item.parent_id = current_job.job_id
            context.job_registry.add(item)

    end_time = datetime.now(tz=timezone.utc)
    timing_info[job_id] = (start_time, end_time)
    context.job_registry.set_state(job_id, JobState.COMPLETED)


def handle_workflow_stall(
    batch_duration: float,
    stall_time: float,
    workflow_id: str,
) -> Iterator[ZahirEvent]:
    """Handle workflow stall period if batch completed faster than stall time.

    @param batch_duration: How long the batch took to execute in seconds
    @param stall_time: The minimum time between workflow phases in seconds
    @param workflow_id: The ID of the workflow
    @return: Iterator of stall events
    """

    if batch_duration < stall_time:
        sleep_time = stall_time - batch_duration
        yield WorkflowStallStartEvent(workflow_id, sleep_time)
        time.sleep(sleep_time)
        yield WorkflowStallEndEvent(workflow_id, sleep_time)


def execute_workflow_batch(
    executor: ThreadPoolExecutor,
    runnable_jobs: list[tuple[str, Job]],
    workflow_id: str,
    context: Context,
) -> Iterator[ZahirEvent]:
    """Execute a batch of runnable jobs in parallel

    @param executor: The thread pool executor to use
    @param runnable_jobs: List of (job_id, job) tuples to execute
    @param registry: The job registry to add subjobs to
    @param workflow_id: The ID of the workflow
    @param context: The context containing scope and registries
    """
    job_futures: dict[Future, tuple[str, Job, float | None, datetime]] = {}
    timing_info: dict[str, tuple[datetime, datetime]] = {}

    # submit all runnable jobs to the executor
    for job_id, current_job in runnable_jobs:
        # First, let's precheck for errors
        precheck_errors = current_job.precheck(current_job.input)
        if precheck_errors:
            yield JobPrecheckFailedEvent(
                workflow_id, current_job, job_id, precheck_errors
            )
            context.job_registry.set_state(job_id, JobState.COMPLETED)
            continue

        # Yield started event and set state before submitting
        yield JobStartedEvent(workflow_id, current_job, job_id)
        context.job_registry.set_state(job_id, JobState.RUNNING)

        timeout = current_job.options.job_timeout if current_job.options else None
        submit_time = datetime.now(tz=timezone.utc)
        future = executor.submit(
            execute_single_job, job_id, current_job, context, timing_info, workflow_id
        )
        job_futures[future] = (job_id, current_job, timeout, submit_time)

    # Wait for all submitted jobs to complete
    for future in as_completed(job_futures):
        job_id, current_job, timeout, submit_time = job_futures[future]

        try:
            # Actually enforce the timeout by passing it to result()
            # This will raise TimeoutError if the job exceeds its timeout
            for event in future.result(timeout=timeout):
                yield event

            # Job completed successfully within timeout
            start_time, end_time = timing_info[job_id]
            duration = (end_time - start_time).total_seconds()
            # State already set to COMPLETED in execute_single_job
            yield JobCompletedEvent(workflow_id, current_job, job_id, duration)
        except TimeoutError:
            # Job exceeded its timeout - cancel it and trigger recovery
            future.cancel()
            completion_time = datetime.now(tz=timezone.utc)
            elapsed = (completion_time - submit_time).total_seconds()

            timeout_err = TimeoutError(
                f"Job {type(current_job).__name__} exceeded timeout of {timeout}s"
            )
            context.job_registry.set_state(job_id, JobState.TIMED_OUT)
            yield JobTimeoutEvent(workflow_id, current_job, job_id, elapsed)
            yield from recover_workflow(
                current_job, job_id, timeout_err, workflow_id, context
            )
        except Exception as job_err:
            # Job raised an exception - trigger recovery
            yield from recover_workflow(
                current_job, job_id, job_err, workflow_id, context
            )


class Workflow:
    """A workflow execution engine"""

    DEFAULT_MAX_WORKERS = 4
    # How long should we wait between workflow phases? (in seconds)
    # By default, we wait five seconds before rechecking for runnable jobs.s
    STALL_TIME = 5

    def __init__(
        self,
        context: Context,
        max_workers: int | None = None,
        stall_time: int | None = None,
    ) -> None:
        """Initialize a workflow execution engine

        @param context: The context containing scope, job registry, and event registry
        @param max_workers: Maximum number of parallel workers (default: DEFAULT_MAX_WORKERS)
        @param stall_time: Time to wait between workflow phases. Wait times may be larger,
            as this includes the length the jobs themselves run for (default: STALL_TIME)
        """

        self.context = context
        self.max_workers = (
            max_workers if max_workers is not None else self.DEFAULT_MAX_WORKERS
        )
        self.stall_time = stall_time if stall_time is not None else self.STALL_TIME

    def _workflow_id(self) -> str:
        """Generate a unique workflow ID based on the current time"""

        return str(uuid.uuid4())

    def _run(
        self, start: Job | None = None, context: Context | None = None
    ) -> Iterator[ZahirEvent]:
        """Run a workflow from the starting job

        @param start: The starting job of the workflow
        @param context: The context to use for job execution (created from scope/registries if not provided)
        """

        workflow_id = self._workflow_id()
        workflow_start_time = datetime.now(tz=timezone.utc)

        # if desired, seed the workflow with an initial job. Otherwise we'll just
        # process jobs currently in the workflow registry.
        if start is not None:
            self.context.job_registry.add(start)

        with ThreadPoolExecutor(max_workers=self.max_workers) as exec:
            while True:
                # Note: this is a bit memory-inefficient.
                runnable_jobs = list(self.context.job_registry.runnable(self.context))

                # Yield information on each job we currently consider runnable.
                for runnable_job_id, runnable in runnable_jobs:
                    yield JobRunnableEvent(workflow_id, runnable, runnable_job_id)

                running_jobs = list(self.context.job_registry.running(self.context))

                # Yield information on each job currently running.
                for running_job_id, running in running_jobs:
                    yield JobRunningEvent(workflow_id, running, running_job_id)

                # We're finished; record we're done and exit.
                if not runnable_jobs and not running_jobs:
                    workflow_end_time = datetime.now(tz=timezone.utc)
                    workflow_duration = (
                        workflow_end_time - workflow_start_time
                    ).total_seconds()

                    # Yield workflow output event
                    yield from self.context.job_registry.outputs(workflow_id)

                    yield WorkflowCompleteEvent(workflow_id, workflow_duration)
                    break

                # Run the batch of jobs that are unblocked across `max_workers` threads.
                batch_start_time = datetime.now(tz=timezone.utc)
                yield from execute_workflow_batch(
                    exec, runnable_jobs, workflow_id, self.context
                )

                batch_end_time = datetime.now(tz=timezone.utc)
                batch_duration = (batch_end_time - batch_start_time).total_seconds()

                # For IO-bound workflows, we're unlikely to need this. But
                # for shorter workflows throttling might be needed.
                yield from handle_workflow_stall(
                    batch_duration, self.stall_time, workflow_id
                )

    def run(self, start: Job | None = None) -> Iterator[ZahirEvent]:
        """Run a workflow from the starting job

        @param start: The starting job of the workflow
        """

        for event in self._run(start):
            self.context.event_registry.register(event)
            yield event
