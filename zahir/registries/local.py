"""Registry management for workflow execution."""

from threading import Lock
from typing import Iterator
from zahir.events import ZahirEvent
from zahir.types import EventRegistry, JobRegistry, Job, ArgsType, DependencyType


class MemoryJobRegistry(JobRegistry):
    """Thread-safe job registry"""

    def __init__(self) -> None:
        self.job_counter: int = 0
        self.pending_jobs: dict[int, Job] = {}
        self.completed_jobs: dict[int, Job] = {}
        self._lock = Lock()

    def add(self, job: "Job[ArgsType, DependencyType]") -> int:
        """Register a job with the job registry, returning a job ID

        @param job: The job to register
        @return: The job ID assigned to the job
        """

        with self._lock:
            self.job_counter += 1
            job_id = self.job_counter
            self.pending_jobs[job_id] = job

        return job_id

    def complete(self, job_id: int) -> int:
        """Mark a job as complete, removing it from the registry.

        @param job_id: The ID of the job to mark as complete
        @return: The ID of the completed job
        """

        with self._lock:
            if job_id in self.pending_jobs:
                job = self.pending_jobs[job_id]
                self.completed_jobs[job_id] = job
                del self.pending_jobs[job_id]

        return job_id

    def pending(self) -> bool:
        """Check whether any jobs still need to be run.

        @return: True if there are pending jobs, False otherwise
        """

        with self._lock:
            return bool(self.pending_jobs)

    def runnable(self) -> Iterator[tuple[int, "Job"]]:
        """Yield all runnable jobs from the registry.

        @return: An iterator of (job ID, job) tuples for runnable jobs
        """

        with self._lock:
            for job_id, job in list(self.pending_jobs.items()):
                if job.ready():
                    yield job_id, job


class MemoryEventRegistry(EventRegistry):
    """Keep track of workflow events in memory."""

    events: list[ZahirEvent]

    def register(self, event: ZahirEvent) -> None:
        """Register an event in the event registry."""

        self.events.append(event)
