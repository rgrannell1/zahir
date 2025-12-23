"""Registry management for workflow execution."""

from dataclasses import dataclass
from threading import Lock
from typing import Iterator
from zahir.events import ZahirEvent
from zahir.types import (
    DependencyState,
    EventRegistry,
    JobRegistry,
    Job,
    ArgsType,
    DependencyType,
    JobState,
)


@dataclass
class JobEntry:
    """Entry in the job registry containing job and its state"""

    job: Job
    state: JobState


class MemoryJobRegistry(JobRegistry):
    """Thread-safe job registry"""

    def __init__(self) -> None:
        self.job_counter: int = 0
        self.jobs: dict[int, JobEntry] = {}
        self._lock = Lock()

    def add(self, job: "Job[ArgsType, DependencyType]") -> int:
        """Register a job with the job registry, returning a job ID

        @param job: The job to register
        @return: The job ID assigned to the job
        """

        with self._lock:
            self.job_counter += 1
            job_id = self.job_counter
            self.jobs[job_id] = JobEntry(job=job, state=JobState.PENDING)

        return job_id

    def set_state(self, job_id: int, state: JobState) -> int:
        """Set the state of a job by ID

        @param job_id: The ID of the job to mark as complete
        @param state: The new state of the job

        @return: The ID of the job
        """

        with self._lock:
            if job_id in self.jobs:
                self.jobs[job_id].state = state

        return job_id

    def pending(self) -> bool:
        """Check whether any jobs still need to be run.

        @return: True if there are pending jobs, False otherwise
        """

        with self._lock:
            return any(
                entry.state == JobState.PENDING for entry in self.jobs.values()
            )

    def runnable(self) -> Iterator[tuple[int, "Job"]]:
        """Yield all runnable jobs from the registry.

        @return: An iterator of (job ID, job) tuples for runnable jobs
        """

        with self._lock:
            runnable_list = []

            for job_id, entry in self.jobs.items():
                if entry.state != JobState.PENDING:
                    continue

                status = entry.job.ready()

                if status == DependencyState.SATISFIED:
                    runnable_list.append((job_id, entry.job))
                elif status == DependencyState.IMPOSSIBLE:
                    # If any dependency is impossible, we can no longer run this job
                    # TODO this should yield an event in some way
                    entry.state = JobState.IMPOSSIBLE

        # Yield outside the lock to avoid holding it during iteration
        for job_id, job in runnable_list:
            yield job_id, job


class MemoryEventRegistry(EventRegistry):
    """Keep track of workflow events in memory."""

    events: list[ZahirEvent]

    def __init__(self) -> None:
        self.events = []

    def register(self, event: ZahirEvent) -> None:
        """Register an event in the event registry."""

        self.events.append(event)
