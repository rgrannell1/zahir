"""Queue management for workflow execution."""

from typing import Any, Iterator, TypeVar
from src.types import JobQueue, Task, Dependency


ArgsType = TypeVar("ArgsType", bound=dict)
DependencyType = TypeVar("DependencyType", bound=Dependency)


class MemoryJobQueue(JobQueue):
    """An in-memory registry of jobs."""

    job_counter: int = 0

    pending_jobs: dict[int, Task] = {}
    completed_jobs: dict[int, Task] = {}

    def add(self, task: "Task[ArgsType, DependencyType]") -> int:
        """Register a task with the job queue, returning a job ID

        @param task: The task to register
        @return: The job ID assigned to the task
        """

        self.job_counter += 1
        job_id = self.job_counter
        self.pending_jobs[job_id] = task

        return job_id

    def complete(self, job_id: int) -> int:
        """Mark a job as complete, removing it from the queue.

        @param job_id: The ID of the job to mark as complete
        @return: The ID of the completed job
        """

        if job_id in self.pending_jobs:
            task = self.pending_jobs[job_id]
            self.completed_jobs[job_id] = task
            del self.pending_jobs[job_id]

        return job_id

    def pending(self) -> bool:
        """Check whether any jobs still need to be run.

        @return: True if there are pending jobs, False otherwise
        """

        return bool(self.pending_jobs)

    def runnable(self) -> Iterator[tuple[int, "Task"]]:
        """Yield all runnable jobs from the queue.

        @return: An iterator of (job ID, task) tuples for runnable jobs
        """

        for job_id, job in self.pending_jobs.items():
            if job.ready():
                yield job_id, job
