"""Queue management for workflow execution.

This module provides local, in-memory implementations of job queues for managing
workflow task execution. These queues track pending and completed jobs, enforce
dependency constraints, and provide thread-safe operations for concurrent workflow
execution.

Classes:
    MemoryJobQueue: Thread-safe in-memory job queue implementation

The queues in this module are designed to work with the workflow execution engine,
managing task lifecycle from registration through completion while respecting task
dependencies and concurrency constraints.
"""

from threading import Lock
from typing import Iterator, TypeVar
from src.types import JobQueue, Task, Dependency


ArgsType = TypeVar("ArgsType", bound=dict)
DependencyType = TypeVar("DependencyType", bound=Dependency)


class MemoryJobQueue(JobQueue):
    """An in-memory, thread-safe registry of jobs for local workflow execution.
    
    This queue manages the lifecycle of tasks in a workflow, tracking which jobs
    are pending, which are ready to run (dependencies satisfied), and which have
    completed. All operations are thread-safe to support parallel task execution.
    
    The queue assigns unique sequential IDs to each task and maintains separate
    collections for pending and completed jobs. Tasks are only returned as runnable
    when their dependencies are satisfied (via task.ready()).
    
    Attributes:
        job_counter: Monotonically increasing counter for assigning job IDs
        pending_jobs: Dictionary mapping job IDs to tasks awaiting execution
        completed_jobs: Dictionary mapping job IDs to finished tasks
    
    Thread Safety:
        All public methods use a lock to ensure thread-safe access to shared state.
    """

    def __init__(self) -> None:
        self.job_counter: int = 0
        self.pending_jobs: dict[int, Task] = {}
        self.completed_jobs: dict[int, Task] = {}
        self._lock = Lock()

    def add(self, task: "Task[ArgsType, DependencyType]") -> int:
        """Register a task with the job queue, returning a job ID

        @param task: The task to register
        @return: The job ID assigned to the task
        """

        with self._lock:
            self.job_counter += 1
            job_id = self.job_counter
            self.pending_jobs[job_id] = task

        return job_id

    def complete(self, job_id: int) -> int:
        """Mark a job as complete, removing it from the queue.

        @param job_id: The ID of the job to mark as complete
        @return: The ID of the completed job
        """

        with self._lock:
            if job_id in self.pending_jobs:
                task = self.pending_jobs[job_id]
                self.completed_jobs[job_id] = task
                del self.pending_jobs[job_id]

        return job_id

    def pending(self) -> bool:
        """Check whether any jobs still need to be run.

        @return: True if there are pending jobs, False otherwise
        """

        with self._lock:
            return bool(self.pending_jobs)

    def runnable(self) -> Iterator[tuple[int, "Task"]]:
        """Yield all runnable jobs from the queue.

        @return: An iterator of (job ID, task) tuples for runnable jobs
        """

        with self._lock:
            for job_id, job in list(self.pending_jobs.items()):
                if job.ready():
                    yield job_id, job
