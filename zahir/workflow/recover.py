"""Workflow execution engine"""

from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, TimeoutError
from typing import Any, Iterator, Mapping, TypeVar, cast

from zahir.events import (
    JobIrrecoverableEvent,
    JobOutputEvent,
    JobRecoveryCompleted,
    JobRecoveryStarted,
    JobRecoveryTimeout,
    WorkflowOutputEvent,
    ZahirEvent,
)
from zahir.base_types import Context, Job, ArgsType, OutputType, JobState

WorkflowOutputType = TypeVar("WorkflowOutputType", bound=Mapping[str, Any])


def recover_workflow(
    current_job: Job[ArgsType, OutputType],
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
                yield JobRecoveryStarted(workflow_id, job_id)
                # Get recovery events from the future
                for event in future.result(timeout=recovery_timeout):
                    yield event
                recovery_end_time = datetime.now(tz=timezone.utc)
                recovery_duration = (
                    recovery_end_time - recovery_start_time
                ).total_seconds()
                context.job_registry.set_state(job_id, JobState.RECOVERED)
                context.job_registry.set_recovery_duration(job_id, recovery_duration)
                yield JobRecoveryCompleted(workflow_id, job_id, recovery_duration)
            except TimeoutError:
                context.job_registry.set_state(job_id, JobState.RECOVERY_TIMED_OUT)
                yield JobRecoveryTimeout(workflow_id, job_id)
    except Exception as recovery_err:
        # Oh dear! Even the recovery failed.
        context.job_registry.set_state(job_id, JobState.IRRECOVERABLE)
        yield JobIrrecoverableEvent(workflow_id, recovery_err, job_id)


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
            context.job_registry.set_output(
                current_job.job_id, cast(Mapping, item.output)
            )
            item.workflow_id = workflow_id
            item.job_id = current_job.job_id
            yield item
            break
        elif isinstance(item, WorkflowOutputEvent):
            # Yield workflow output event directly
            item.workflow_id = workflow_id
            yield item
        else:
            # Only add Jobs to the job registry
            from zahir.base_types import Job

            if isinstance(item, Job):
                context.job_registry.add(item)
