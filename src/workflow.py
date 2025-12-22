"""Workflow execution engine"""

from typing import Iterator, TypeVar
from queues.local import JobQueue, MemoryJobQueue
from src.types import Task, Dependency

ArgsType = TypeVar("ArgsType", bound=dict)
DependencyType = TypeVar("DependencyType", bound=Dependency)


def find_runnable_jobs(
    jobs: list[Task[ArgsType, DependencyType]],
) -> Iterator[Task[ArgsType, DependencyType]]:
    """Which jobs can be run? Check, in series, which jobs have all their dependencies.

    For the moment, let's do this is a single-threaded scan to minimise
    race-conditions.
    """

    for job in jobs:
        if isinstance(job.dependencies, dict):
            ready_to_go = all(dep.satisfied() for dep in job.dependencies.values())
        elif hasattr(job.dependencies, "__dict__"):
            # Handle dataclass dependencies
            ready_to_go = all(
                dep.satisfied()
                for dep in vars(job.dependencies).values()
                if hasattr(dep, "satisfied")
            )
        else:
            # Assume it's an iterable of dependencies
            ready_to_go = True

        if ready_to_go:
            yield job


def recover_workflow(
    current_job: Task[ArgsType, DependencyType], queue: JobQueue, err: Exception
):
    """Attempt to recover from a failed job by invoking its recovery method."""

    try:
        exc_jobs = current_job.recover(err)

        for exc_job in exc_jobs:
            queue.add(exc_job)
    except Exception as recovery_err:
        # Oh dear! Even the recovery failed.

        print(
            f"Recovery from job {type(current_job).__name__} failed: {recovery_err}"
        )



def execute_workflow(start: Task[ArgsType, DependencyType]) -> None:
    """Execute each step of the workflow, persist state centrally. Jobs
    are run in parallel but scheduled synchronously to avoid race-conditions in
    dependencies"""

    queue = MemoryJobQueue()
    queue.add(start)

    while not queue.pending():
        runnable_jobs = queue.runnable()

        for job_id, current_job in runnable_jobs:
            try:
                for subjob in current_job.run():
                    queue.add(subjob)
                queue.complete(job_id)
            except Exception as job_err:
                recover_workflow(current_job, queue, job_err)


class Workflow:
    def __init__(self) -> None:
        """Initialize a workflow execution engine"""
        pass

    def run(self, start: Task[ArgsType, DependencyType]) -> None:
        """Run a workflow from the starting task

        @param start: The starting task of the workflow
        """

        execute_workflow(start)
