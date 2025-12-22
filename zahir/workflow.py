"""Workflow execution engine"""

from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError
from zahir.queues.local import JobQueue, MemoryJobQueue
from zahir.exception import TimeoutException, UnrecoverableJobException
from zahir.types import Task, ArgsType, DependencyType


def recover_workflow(
    current_job: Task[ArgsType, DependencyType], queue: JobQueue, err: Exception
):
    """Attempt to recover from a failed job by invoking its recovery method

    @param current_job: The job that failed
    @param queue: The job queue to add recovery jobs to
    @param err: The exception that caused the failure
    """

    timeout = current_job.RECOVER_TIMEOUT

    try:
        if timeout is None:
            _run_recovery(current_job, err, queue)
            return

        # hacky method to allow timeouts
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(_run_recovery, current_job, err, queue)

            try:
                future.result(timeout=timeout)
            except TimeoutError:
                raise TimeoutException(
                    f"Recovery for job {type(current_job).__name__} timed out after {timeout}s"
                )

    except TimeoutException:
        raise
    except Exception as recovery_err:
        # Oh dear! Even the recovery failed. I guess we need to bail to the workflow engine.

        raise UnrecoverableJobException(
            f"Recovery for job {type(current_job).__name__} failed"
        ) from recovery_err


def _run_recovery(current_job: Task, err: Exception, queue: JobQueue) -> None:
    """Execute the recovery process for a failed job.

    @param current_job: The job that failed
    @param err: The exception that caused the failure
    @param queue: The job queue to add recovery jobs to
    """
    exc_jobs = current_job.recover(err)

    for exc_job in exc_jobs:
        queue.add(exc_job)


def execute_job(job_id: int, current_job: Task, queue: JobQueue) -> None:
    """Execute a single job and handle its subjobs

    @param job_id: The ID of the job to execute
    @param current_job: The task to execute
    @param queue: The job queue to add subjobs to
    """

    for subjob in current_job.run():
        queue.add(subjob)
    queue.complete(job_id)


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
    job_futures = {}
    for job_id, current_job in runnable_jobs:
        future = executor.submit(execute_job, job_id, current_job, queue)
        job_futures[future] = (job_id, current_job)

    # Wait for all submitted jobs to complete
    for future in as_completed(job_futures):
        job_id, current_job = job_futures[future]
        timeout = current_job.TASK_TIMEOUT
        try:
            future.result(timeout=timeout)
        except TimeoutError:
            timeout_err = TimeoutError(
                f"Task {type(current_job).__name__} timed out after {timeout}s"
            )
            recover_workflow(current_job, queue, timeout_err)
        except Exception as job_err:
            recover_workflow(current_job, queue, job_err)


class Workflow:
    """A workflow execution engine"""

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
