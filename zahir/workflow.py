"""Workflow execution engine"""

import time
from datetime import datetime, timezone
from concurrent.futures import Future, ThreadPoolExecutor, as_completed, TimeoutError
from typing import Iterator

from zahir.events import (
    JobCompletedEvent,
    JobIrrecoverableEvent,
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
from zahir.types import Job, ArgsType, DependencyType


def recover_workflow(
    current_job: Job[ArgsType, DependencyType],
    job_id: int,
    registry: JobRegistry,
    err: Exception,
) -> Iterator[ZahirEvent]:
    """Attempt to recover from a failed job by invoking its recovery method

    @param current_job: The job that failed
    @param job_id: The ID of the job that failed
    @param registry: The job registry to add recovery jobs to
    @param err: The exception that caused the failure
    """

    recovery_timeout = current_job.RECOVER_TIMEOUT

    try:
        # hacky method to handle recovery timeouts
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(_run_recovery, current_job, err, registry)

            try:
                recovery_start_time = datetime.now(tz=timezone.utc)
                yield JobRecoveryStarted(current_job, job_id)
                future.result(timeout=recovery_timeout)
                recovery_end_time = datetime.now(tz=timezone.utc)
                recovery_duration = (
                    recovery_end_time - recovery_start_time
                ).total_seconds()
                yield JobRecoveryCompleted(current_job, job_id, recovery_duration)
            except TimeoutError:
                yield JobRecoveryTimeout(current_job, job_id)
    except Exception as recovery_err:
        # Oh dear! Even the recovery failed.
        yield JobIrrecoverableEvent(recovery_err, current_job, job_id)


def _run_recovery(current_job: Job, err: Exception, registry: JobRegistry) -> None:
    """Execute the recovery process for a failed job.

    @param current_job: The job that failed
    @param err: The exception that caused the failure
    @param registry: The job registry to add recovery jobs to
    """

    for exc_job in current_job.recover(err):
        registry.add(exc_job)


def execute_single_job(job_id: int, current_job: Job, registry: JobRegistry) -> None:
    """Execute a single job and handle its subjobs

    @param job_id: The ID of the job to execute
    @param current_job: The job to execute
    @param registry: The job registry to add subjobs to
    """

    for subjob in current_job.run():
        registry.add(subjob)
    registry.complete(job_id)


def execute_workflow_batch(
    executor: ThreadPoolExecutor,
    runnable_jobs: list[tuple[int, Job]],
    registry: JobRegistry,
) -> Iterator[ZahirEvent]:
    """Execute a batch of runnable jobs in parallel

    @param executor: The thread pool executor to use
    @param runnable_jobs: List of (job_id, job) tuples to execute
    @param registry: The job registry to add subjobs to
    """
    job_futures: dict[Future, tuple[int, Job]] = {}

    # submit all runnable jobs to the executor
    for job_id, current_job in runnable_jobs:
        future = executor.submit(execute_single_job, job_id, current_job, registry)
        job_futures[future] = (job_id, current_job)

    # Wait for all submitted jobs to complete
    for future in as_completed(job_futures):
        job_id, current_job = job_futures[future]
        timeout = current_job.JOB_TIMEOUT
        job_start_time = datetime.now(tz=timezone.utc)
        try:
            yield JobStartedEvent(current_job, job_id)
            future.result(timeout=timeout)
            job_end_time = datetime.now(tz=timezone.utc)
            duration = (job_end_time - job_start_time).total_seconds()
            yield JobCompletedEvent(current_job, job_id, duration)
        except TimeoutError:
            job_end_time = datetime.now(tz=timezone.utc)
            duration = (job_end_time - job_start_time).total_seconds()
            timeout_err = TimeoutError(
                f"Job {type(current_job).__name__} timed out after {timeout}s"
            )
            yield JobTimeoutEvent(current_job, job_id, duration)
            yield from recover_workflow(current_job, job_id, registry, timeout_err)
        except Exception as job_err:
            yield from recover_workflow(current_job, job_id, registry, job_err)


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

    def _run(self, start: Job | None = None) -> Iterator[ZahirEvent]:
        """Run a workflow from the starting job

        @param start: The starting job of the workflow
        """

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
                    yield JobRunnableEvent(runnable, runnable_job_id)

                # We're finished
                if not runnable_jobs:
                    workflow_end_time = datetime.now(tz=timezone.utc)
                    workflow_duration = (
                        workflow_end_time - workflow_start_time
                    ).total_seconds()
                    yield WorkflowCompleteEvent(workflow_duration)
                    break

                # Run the batch of jobs that are unblocked across `max_workers` threads.
                batch_start_time = datetime.now(tz=timezone.utc)
                yield from execute_workflow_batch(
                    exec, runnable_jobs, self.job_registry
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
