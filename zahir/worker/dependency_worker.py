import multiprocessing
import os
import time

from zahir.base_types import Context, DependencyState, JobState
from zahir.events import (
    JobReadyEvent,
    WorkflowCompleteEvent,
    ZahirEvent,
    ZahirInternalErrorEvent,
)
from zahir.exception import ImpossibleDependencyError, exception_to_text_blob
from zahir.serialise import serialise_event
from zahir.utils.logging_config import configure_logging

type OutputQueue = multiprocessing.Queue["ZahirEvent"]


def zahir_dependency_worker(context: Context, output_queue: OutputQueue, workflow_id: str) -> None:
    """Analyse job dependencies and mark jobs as pending."""

    # Configure logging for this worker process
    configure_logging()

    try:
        job_registry = context.job_registry
        job_registry.init(str(os.getpid()))

        while True:
            if not job_registry.is_active():
                duration = job_registry.get_workflow_duration() or 0.0
                output_queue.put(
                    serialise_event(
                        context,
                        WorkflowCompleteEvent(
                            workflow_id=workflow_id,
                            duration_seconds=duration,
                        )
                    )
                )
                return

            any_ready = False

            # try to find blocked jobs whose dependencies are now satisfied
            for job_info in job_registry.jobs(context, state=JobState.PENDING):
                job = job_info.job

                dependencies_state = job.dependencies.satisfied()

                if dependencies_state == DependencyState.SATISFIED:
                    job_registry.set_state(context, job.job_id, job.spec.type, workflow_id, output_queue, JobState.READY, recovery=False)
                    any_ready = True
                elif dependencies_state == DependencyState.IMPOSSIBLE:
                    # Set an error, as awaited jobs cannot be run if impossible dependencies are present.

                    error = ImpossibleDependencyError(f"Job {job.job_id} has impossible dependencies.")
                    job_registry.set_state(
                        context, job.job_id, job.spec.type, workflow_id, output_queue, JobState.IMPOSSIBLE, error=error, recovery=False
                    )

            # Also check if there are any READY jobs that need dispatching
            # (includes jobs that started as READY because they have no dependencies)
            for _ in job_registry.jobs(context, state=JobState.READY):
                any_ready = True
                break

            if any_ready:
                # Let the overseer know that there are jobs ready to run
                output_queue.put(serialise_event(context, JobReadyEvent()))

            time.sleep(1)

    except Exception as err:
        output_queue.put(
            serialise_event(
                context,
                ZahirInternalErrorEvent(
                    workflow_id=workflow_id,
                    error=exception_to_text_blob(err),
                )
            )
        )
