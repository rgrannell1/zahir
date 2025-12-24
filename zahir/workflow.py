"""Workflow execution engine"""

import time
from datetime import datetime, timezone
from concurrent.futures import Future, ThreadPoolExecutor, as_completed, TimeoutError
from typing import Iterator

from zahir.events import (
    JobCompletedEvent,
    JobIrrecoverableEvent,
    JobPrecheckFailedEvent,
    JobRecoveryCompleted,
    JobRecoveryStarted,
    JobRecoveryTimeout,
    JobRunnableEvent,
    JobStartedEvent,
    JobTimeoutEvent,
    WorkflowCompleteEvent,
    ZahirEvent,
)
from zahir.registries.local import JobRegistry, MemoryEventRegistry, MemoryJobRegistry
from zahir.types import Job, ArgsType, DependencyType, JobState
import uuid


def recover_workflow(
    current_job: Job[ArgsType, DependencyType],
    job_id: int,
    registry: JobRegistry,
    err: Exception,
    workflow_id: str,
) -> Iterator[ZahirEvent]:
    """Attempt to recover from a failed job by invoking its recovery method

    @param current_job: The job that failed
    @param job_id: The ID of the job that failed
    @param registry: The job registry to add recovery jobs to
    @param err: The exception that caused the failure
    @param workflow_id: The ID of the workflow
    """

    recovery_timeout = (
        current_job.options.recover_timeout if current_job.options else None
    )

    try:
        # hacky method to handle recovery timeouts
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(_run_recovery, current_job, err, registry)

            try:
                recovery_start_time = datetime.now(tz=timezone.utc)
                registry.set_state(job_id, JobState.RECOVERING)
                yield JobRecoveryStarted(workflow_id, current_job, job_id)
                future.result(timeout=recovery_timeout)
                recovery_end_time = datetime.now(tz=timezone.utc)
                recovery_duration = (
                    recovery_end_time - recovery_start_time
                ).total_seconds()
                registry.set_state(job_id, JobState.RECOVERED)
                yield JobRecoveryCompleted(
                    workflow_id, current_job, job_id, recovery_duration
                )
            except TimeoutError:
                registry.set_state(job_id, JobState.RECOVERY_TIMED_OUT)
                yield JobRecoveryTimeout(workflow_id, current_job, job_id)
    except Exception as recovery_err:
        # Oh dear! Even the recovery failed.
        registry.set_state(job_id, JobState.IRRECOVERABLE)
        yield JobIrrecoverableEvent(workflow_id, recovery_err, current_job, job_id)


def _run_recovery(current_job: Job, err: Exception, registry: JobRegistry) -> None:
    """Execute the recovery process for a failed job.

    @param current_job: The job that failed
    @param err: The exception that caused the failure
    @param registry: The job registry to add recovery jobs to
    """

    for exc_job in current_job.recover(err):
        registry.add(exc_job)


def execute_single_job(
    job_id: int,
    current_job: Job,
    registry: JobRegistry,
    timing_info: dict[int, tuple[datetime, datetime]],
) -> None:
    """Execute a single job and handle its subjobs

    @param job_id: The ID of the job to execute
    @param current_job: The job to execute
    @param registry: The job registry to add subjobs to
    @param timing_info: Dictionary to store (start_time, end_time) tuples by job_id
    """

    start_time = datetime.now(tz=timezone.utc)
    for subjob in current_job.run():
        subjob.parent = current_job
        registry.add(subjob)

    end_time = datetime.now(tz=timezone.utc)
    timing_info[job_id] = (start_time, end_time)
    registry.set_state(job_id, JobState.COMPLETED)


def execute_workflow_batch(
    executor: ThreadPoolExecutor,
    runnable_jobs: list[tuple[int, Job]],
    registry: JobRegistry,
    workflow_id: str,
) -> Iterator[ZahirEvent]:
    """Execute a batch of runnable jobs in parallel

    @param executor: The thread pool executor to use
    @param runnable_jobs: List of (job_id, job) tuples to execute
    @param registry: The job registry to add subjobs to
    @param workflow_id: The ID of the workflow
    """
    job_futures: dict[Future, tuple[int, Job]] = {}
    timing_info: dict[int, tuple[datetime, datetime]] = {}

    # submit all runnable jobs to the executor
    for job_id, current_job in runnable_jobs:
        # First, let's precheck for errors
        precheck_errors = current_job.precheck(current_job.input)
        if precheck_errors:
            yield JobPrecheckFailedEvent(
                workflow_id, current_job, job_id, precheck_errors
            )
            registry.set_state(job_id, JobState.COMPLETED)
            continue

        future = executor.submit(
            execute_single_job, job_id, current_job, registry, timing_info
        )
        job_futures[future] = (job_id, current_job)

    # Wait for all submitted jobs to complete
    for future in as_completed(job_futures):
        job_id, current_job = job_futures[future]
        timeout = current_job.options.job_timeout if current_job.options else None
        try:
            registry.set_state(job_id, JobState.RUNNING)
            yield JobStartedEvent(workflow_id, current_job, job_id)
            future.result(timeout=timeout)
            # Get actual execution timing from the worker thread
            start_time, end_time = timing_info[job_id]
            duration = (end_time - start_time).total_seconds()
            # State already set to COMPLETED in execute_single_job
            yield JobCompletedEvent(workflow_id, current_job, job_id, duration)
        except TimeoutError:
            # Job timed out - use timeout value as duration if available
            duration = float(timeout) if timeout is not None else 0.0
            timeout_err = TimeoutError(
                f"Job {type(current_job).__name__} timed out after {timeout}s"
            )
            registry.set_state(job_id, JobState.TIMED_OUT)
            yield JobTimeoutEvent(workflow_id, current_job, job_id, duration)
            yield from recover_workflow(
                current_job, job_id, registry, timeout_err, workflow_id
            )
        except Exception as job_err:
            yield from recover_workflow(
                current_job, job_id, registry, job_err, workflow_id
            )


class Workflow:
    """A workflow execution engine"""

    DEFAULT_MAX_WORKERS = 4
    # How long should we wait between workflow phases? (in seconds)
    # By default, we wait five seconds before rechecking for runnable jobs.s
    STALL_TIME = 5

    def __init__(
        self,
        job_registry: JobRegistry | None = None,
        event_registry: MemoryEventRegistry | None = None,
        max_workers: int | None = None,
        stall_time: int | None = None,
    ) -> None:
        """Initialize a workflow execution engine

        @param registry: The job registry to use for managing jobs
        @param max_workers: Maximum number of parallel workers (default: DEFAULT_MAX_WORKERS)
        @param stall_time: Time to wait between workflow phases. Wait times may be larger,
            as this includes the length the jobs themselves run for (default: STALL_TIME)
        """

        self.job_registry = (
            job_registry if job_registry is not None else MemoryJobRegistry()
        )
        self.event_registry = (
            event_registry if event_registry is not None else MemoryEventRegistry()
        )
        self.max_workers = (
            max_workers if max_workers is not None else self.DEFAULT_MAX_WORKERS
        )
        self.stall_time = stall_time if stall_time is not None else self.STALL_TIME

    def _workflow_id(self) -> str:
        """Generate a unique workflow ID based on the current time"""

        return str(uuid.uuid4())

    def _run(self, start: Job | None = None) -> Iterator[ZahirEvent]:
        """Run a workflow from the starting job

        @param start: The starting job of the workflow
        """

        workflow_id = self._workflow_id()
        workflow_start_time = datetime.now(tz=timezone.utc)

        # if desired, seed the workflow with an initial job. Otherwise we'll just
        # process jobs currently in the workflow registry.
        if start is not None:
            self.job_registry.add(start)

        with ThreadPoolExecutor(max_workers=self.max_workers) as exec:
            while self.job_registry.pending():
                # Note: this is a bit memory-inefficient.
                runnable_jobs = list(self.job_registry.runnable())

                # Yield information on each job we currently consider runnable.
                for runnable_job_id, runnable in runnable_jobs:
                    yield JobRunnableEvent(workflow_id, runnable, runnable_job_id)

                # We're finished
                if not runnable_jobs:
                    workflow_end_time = datetime.now(tz=timezone.utc)
                    workflow_duration = (
                        workflow_end_time - workflow_start_time
                    ).total_seconds()
                    yield WorkflowCompleteEvent(workflow_id, workflow_duration)
                    break

                # Run the batch of jobs that are unblocked across `max_workers` threads.
                batch_start_time = datetime.now(tz=timezone.utc)
                yield from execute_workflow_batch(
                    exec, runnable_jobs, self.job_registry, workflow_id
                )
                batch_end_time = datetime.now(tz=timezone.utc)
                batch_duration = (batch_end_time - batch_start_time).total_seconds()

                # for IO-bound workflows, we're unlikely to need this. But
                # for shorter workflows throttling might be needed.
                if batch_duration < self.stall_time:
                    sleep_time = self.stall_time - batch_duration
                    time.sleep(sleep_time)

    def run(self, start: Job | None = None):
        """Run a workflow from the starting job

        @param start: The starting job of the workflow
        """

        for event in self._run(start):
            self.event_registry.register(event)
