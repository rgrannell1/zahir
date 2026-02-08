import atexit
from multiprocessing.queues import Queue
import os
import time

from zahir.base_types import Context, DependencyState, JobState
from zahir.constants import DEPENDENCY_LOOP_STALL_SECONDS
from zahir.events import (
    JobReadyEvent,
    WorkflowCompleteEvent,
    ZahirInternalErrorEvent,
)
from zahir.exception import ImpossibleDependencyError, exception_to_text_blob
from zahir.serialise import SerialisedEvent, serialise_event
from zahir.utils.logging_config import configure_logging
from zahir.utils.output_logging import setup_output_logging

type OutputQueue = Queue[SerialisedEvent]


def setup_dependency_worker(
    context: Context,
    log_dir: str | None = None,
) -> None:
    """Configure logging and output logging for the dependency worker."""

    configure_logging()

    if log_dir:
        setup_output_logging(log_dir=log_dir)


def _setup_job_registry(context: Context) -> None:
    """Initialize the job registry and register cleanup on exit."""

    job_registry = context.job_registry
    job_registry.init(str(os.getpid()))
    atexit.register(job_registry.close)


def workflow_completed(
    job_registry,
    workflow_id: str,
) -> bool:
    """
    Check if the workflow is complete and handle completion if so.

    Returns:
        True if workflow is complete and worker should exit, False otherwise.
    """
    current_workflow_complete = not job_registry.is_active(workflow_id=workflow_id)
    active_workflow_ids = job_registry.get_active_workflow_ids()

    return current_workflow_complete and not active_workflow_ids


def job_dependencies_satisfied(
    context: Context,
    output_queue: OutputQueue,
    job_registry,
    job,
    workflow_id: str,
) -> bool:
    """
    Process a single pending job by checking its dependencies.

    Returns:
        True if the job became ready, False otherwise.
    """
    dependencies_state = job.dependencies.satisfied()

    if dependencies_state == DependencyState.SATISFIED:
        job_registry.set_state(
            context, job.job_id, job.spec.type, workflow_id, output_queue, JobState.READY, recovery=False
        )
        return True
    if dependencies_state == DependencyState.IMPOSSIBLE:
        error = ImpossibleDependencyError(f"Job {job.job_id} has impossible dependencies.")
        job_registry.set_state(
            context,
            job.job_id,
            job.spec.type,
            workflow_id,
            output_queue,
            JobState.IMPOSSIBLE,
            error=error,
            recovery=False,
        )
    return False


def has_ready_jobs(context: Context, job_registry, workflow_id: str) -> bool:
    """Check if there are any READY jobs in the workflow."""
    for _ in job_registry.jobs(context, state=JobState.READY, workflow_id=workflow_id):
        return True

    return False


def has_active_jobs(
    context: Context,
    output_queue: OutputQueue,
    job_registry,
    workflow_id: str,
) -> bool:
    """
    Process all jobs for a specific workflow.

    Returns:
        True if any jobs became ready, False otherwise.
    """
    any_ready = False

    # Process pending jobs
    for job_info in job_registry.jobs(context, state=JobState.PENDING, workflow_id=workflow_id):
        job = job_info.job
        if job_dependencies_satisfied(context, output_queue, job_registry, job, workflow_id):
            any_ready = True

    # Check for existing ready jobs
    if has_ready_jobs(context, job_registry, workflow_id):
        any_ready = True

    return any_ready


def has_active_workflows(
    context: Context,
    output_queue: OutputQueue,
    job_registry,
    workflow_id: str,
    active_workflow_ids: list[str],
) -> bool:
    """
    Process jobs from all active workflows.

    Returns:
        True if any jobs became ready
    """
    workflows = {workflow_id} | set(active_workflow_ids)

    for workflow in workflows:
        if has_active_jobs(context, output_queue, job_registry, workflow):
            return True

    return False


def handle_exception(
    context: Context,
    output_queue: OutputQueue,
    workflow_id: str,
    err: Exception,
) -> None:
    """Sent error event on error."""

    output_queue.put(
        serialise_event(
            context,
            ZahirInternalErrorEvent(
                workflow_id=workflow_id,
                error=exception_to_text_blob(err),
            ),
        )
    )


def zahir_dependency_worker(
    context: Context,
    output_queue: OutputQueue,
    workflow_id: str,
    log_dir: str | None = None,
) -> None:
    """Analyse job dependencies and mark jobs as pending."""
    setup_dependency_worker(context, log_dir=log_dir)

    try:
        _setup_job_registry(context)
        job_registry = context.job_registry

        while True:
            if workflow_completed(job_registry, workflow_id):
                duration = job_registry.get_workflow_duration(workflow_id=workflow_id) or 0.0
                output_queue.put(
                    serialise_event(
                        context,
                        WorkflowCompleteEvent(
                            workflow_id=workflow_id,
                            duration_seconds=duration,
                        ),
                    )
                )
                return

            active_workflow_ids = job_registry.get_active_workflow_ids()
            any_ready = has_active_workflows(context, output_queue, job_registry, workflow_id, active_workflow_ids)

            if any_ready:
                output_queue.put(serialise_event(context, JobReadyEvent()))

            time.sleep(DEPENDENCY_LOOP_STALL_SECONDS)

    except Exception as err:
        handle_exception(context, output_queue, workflow_id, err)
