from tblib import pickling_support  # type: ignore[import-untyped]

from zahir.exception import exception_to_text_blob

pickling_support.install()

import multiprocessing
import time

from zahir.base_types import Context, DependencyState, JobState
from zahir.events import (
    WorkflowCompleteEvent,
    ZahirEvent,
    ZahirInternalErrorEvent,
)

type OutputQueue = multiprocessing.Queue["ZahirEvent"]


def zahir_dependency_worker(context: Context, output_queue: OutputQueue, workflow_id: str) -> None:
    """Analyse job dependencies and mark jobs as pending."""

    try:
        while True:
            if not context.job_registry.active():
                output_queue.put(
                    WorkflowCompleteEvent(
                        workflow_id=workflow_id,
                        duration_seconds=0.0,  # TODO: track duration
                    )
                )
                return

            # try to find blocked jobs whose dependencies are now satisfied
            for job_info in context.job_registry.jobs(context, state=JobState.PENDING):
                job = job_info.job

                dependencies_state = job.dependencies.satisfied()

                if dependencies_state == DependencyState.SATISFIED:
                    context.job_registry.set_state(job.job_id, workflow_id, output_queue, JobState.READY)
                elif dependencies_state == DependencyState.IMPOSSIBLE:
                    context.job_registry.set_state(job.job_id, workflow_id, output_queue, JobState.IMPOSSIBLE)

            time.sleep(1)
    except Exception as err:
        output_queue.put(
            ZahirInternalErrorEvent(
                workflow_id=workflow_id,
                error=exception_to_text_blob(err),
            )
        )
