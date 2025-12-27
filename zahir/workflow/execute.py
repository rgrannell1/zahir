"""Workflow execution engine"""

import time
from typing import cast
from datetime import datetime, timezone
from concurrent.futures import Future, ThreadPoolExecutor, as_completed, TimeoutError
from typing import Any, Iterator, Mapping, TypeVar

from zahir.events import (
    JobCompletedEvent,
    JobEvent,
    JobOutputEvent,
    JobPrecheckFailedEvent,
    JobStartedEvent,
    JobTimeoutEvent,
    WorkflowOutputEvent,
    ZahirEvent,
)
from zahir.base_types import Context, Job, JobState
from zahir.workflow.recover import recover_workflow

WorkflowOutputType = TypeVar("WorkflowOutputType", bound=Mapping[str, Any])


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

            # context.job_registry.set_output(job_id, cast(Mapping, item.output))
            item.workflow_id = workflow_id
            item.job_id = job_id

            yield item
            break
        elif isinstance(item, WorkflowOutputEvent):
            # Yield workflow output event directly
            item.workflow_id = workflow_id
            yield item
        elif isinstance(item, Job):
            yield JobEvent(
                job=item.save(),
            )

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
            yield JobPrecheckFailedEvent(workflow_id, job_id, precheck_errors)
            context.job_registry.set_state(job_id, JobState.COMPLETED)
            continue

        # Yield started event and set state before submitting
        submit_time = datetime.now(tz=timezone.utc)
        yield JobStartedEvent(workflow_id, job_id)
        context.job_registry.set_state(job_id, JobState.RUNNING)
        context.job_registry.set_timing(job_id, started_at=submit_time)

        timeout = current_job.options.job_timeout if current_job.options else None
        future = executor.submit(
            execute_single_job, job_id, current_job, context, timing_info, workflow_id
        )
        job_futures[future] = (job_id, current_job, timeout, submit_time)

    # Wait for all submitted jobs to complete
    for future in as_completed(job_futures):
        result_tuple = job_futures[future]
        job_id, current_job, timeout, submit_time = result_tuple

        try:
            # Actually enforce the timeout by passing it to result()
            # This will raise TimeoutError if the job exceeds its timeout
            for event in future.result(timeout=timeout):
                yield event

            # Job completed successfully within timeout
            start_time, end_time = timing_info[job_id]
            duration = (end_time - start_time).total_seconds()
            context.job_registry.set_timing(
                job_id, completed_at=end_time, duration_seconds=duration
            )
            # State already set to COMPLETED in execute_single_job
            yield JobCompletedEvent(workflow_id, job_id, duration)
        except TimeoutError:
            # Job exceeded its timeout - cancel it and trigger recovery
            future.cancel()
            completion_time = datetime.now(tz=timezone.utc)
            elapsed = (completion_time - submit_time).total_seconds()

            timeout_err = TimeoutError(
                f"Job {type(current_job).__name__} exceeded timeout of {timeout}s"
            )
            context.job_registry.set_state(job_id, JobState.TIMED_OUT)
            context.job_registry.set_timing(
                job_id, completed_at=completion_time, duration_seconds=elapsed
            )
            yield JobTimeoutEvent(workflow_id, job_id, elapsed)
            yield from recover_workflow(
                current_job, job_id, timeout_err, workflow_id, context
            )
        except Exception as job_err:
            # Job raised an exception - trigger recovery
            yield from recover_workflow(
                current_job, job_id, job_err, workflow_id, context
            )
