
import multiprocessing
import time
from zahir.base_types import DependencyState, JobState, Scope
from zahir.context.memory import MemoryContext
from zahir.dependencies import job
from zahir.events import (
    ZahirEvent,
)
from zahir.job_registry.sqlite import SQLiteJobRegistry

type OutputQueue = multiprocessing.Queue["ZahirEvent"]

def zahir_dependency_worker(
    scope: Scope, output_queue: OutputQueue, workflow_id: str
) -> None:
    """Analyse job dependencies and mark jobs as pending."""
    ...

    # bad, dependency injection. temporary.
    job_registry = SQLiteJobRegistry("jobs.db")
    context = MemoryContext(scope=scope, job_registry=job_registry)

    while True:
        # try to find blocked jobs whose dependencies are now satisfied
        for job_info in job_registry.jobs(context, state=JobState.BLOCKED):
            ...
            job = job_info.job

            dependencies_state = job.dependencies.satisfied()

            if dependencies_state == DependencyState.SATISFIED:
                job_registry.set_state(job.job_id, JobState.PENDING)
            elif dependencies_state == DependencyState.IMPOSSIBLE:
                job_registry.set_state(job.job_id, JobState.IMPOSSIBLE)

        time.sleep(1)
