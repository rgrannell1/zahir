"""Workflow execution engine"""

from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import TypeVar
from queues.local import JobQueue, MemoryJobQueue
from src.types import Task, Dependency, ArgsType, DependencyType


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

        print(f"Recovery from job {type(current_job).__name__} failed: {recovery_err}")


def execute_job(job_id: int, current_job: Task, queue: JobQueue) -> None:
    """Execute a single job and handle its subjobs

    @param job_id: The ID of the job to execute
    @param current_job: The task to execute
    @param queue: The job queue to add subjobs to
    """
    try:
        for subjob in current_job.run():
            queue.add(subjob)
        queue.complete(job_id)
    except Exception:
        # Re-raise to be caught by the future.result() call
        raise


def execute_batch(
    executor: ThreadPoolExecutor,
    runnable_jobs: list[tuple[int, Task]],
    queue: JobQueue,
) -> None:
    """Execute a batch of runnable jobs in parallel

    @param executor: The thread pool executor to use
    @param runnable_jobs: List of (job_id, task) tuples to execute
    @param queue: The job queue to add subjobs to
    """
    # Submit all runnable jobs to the executor
    futures = {}
    for job_id, current_job in runnable_jobs:
        future = executor.submit(execute_job, job_id, current_job, queue)
        futures[future] = (job_id, current_job)

    # Wait for all submitted jobs to complete
    for future in as_completed(futures):
        job_id, current_job = futures[future]
        try:
            future.result()
        except Exception as job_err:
            recover_workflow(current_job, queue, job_err)


class Workflow:
    DEFAULT_MAX_WORKERS = 4

    def __init__(
        self, queue: JobQueue | None = None, max_workers: int | None = None
    ) -> None:
        """Initialize a workflow execution engine

        @param queue: The job queue to use for managing jobs
        @param max_workers: Maximum number of parallel workers (default: DEFAULT_MAX_WORKERS)
        """

        self.queue = queue if queue is not None else MemoryJobQueue()
        self.max_workers = (
            max_workers if max_workers is not None else self.DEFAULT_MAX_WORKERS
        )

    def run(self, start: Task) -> None:
        """Run a workflow from the starting task

        @param start: The starting task of the workflow
        """

        self.queue.add(start)

        with ThreadPoolExecutor(max_workers=self.max_workers) as exec:
            while self.queue.pending():
                runnable_jobs = list(self.queue.runnable())

                if not runnable_jobs:
                    break

                execute_batch(exec, runnable_jobs, self.queue)
