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
    Scope,
)


@dataclass
class JobEntry:
    """Entry in the job registry containing job and its state"""

    job: Job
    state: JobState


class MemoryJobRegistry(JobRegistry):
    """Thread-safe job registry"""

    def __init__(self, scope: Scope | None = None) -> None:
        self.jobs: dict[str, JobEntry] = {}
        self.outputs: dict[str, dict] = {}
        self._lock = Lock()
        # for consistency, we should probably accept a scope.
        # but I don't think we actually use it given we don't need to serialise and deserialise.
        self.scope = scope

    def add(self, job: "Job[ArgsType, DependencyType]") -> str:
        """Register a job with the job registry, returning a job ID

        @param job: The job to register
        @return: The job ID assigned to the job
        """

        with self._lock:
            job_id = job.job_id
            self.jobs[job_id] = JobEntry(job=job, state=JobState.PENDING)

        return job_id

    def get_state(self, job_id: str) -> JobState:
        """Get the state of a job by ID

        @param job_id: The ID of the job to get the state of

        @return: The state of the job
        """

        with self._lock:
            if job_id in self.jobs:
                return self.jobs[job_id].state
            else:
                raise KeyError(f"Job ID {job_id} not found in registry")

    def set_state(self, job_id: str, state: JobState) -> str:
        """Set the state of a job by ID

        @param job_id: The ID of the job to update
        @param state: The new state of the job

        @return: The ID of the job
        """

        with self._lock:
            if job_id in self.jobs:
                self.jobs[job_id].state = state

        return job_id

    def set_output(self, job_id: str, output: dict) -> None:
        """Store the output of a completed job

        @param job_id: The ID of the job
        @param output: The output dictionary produced by the job
        """

        with self._lock:
            self.outputs[job_id] = output

    def get_output(self, job_id: str) -> dict | None:
        """Retrieve the output of a completed job

        @param job_id: The ID of the job
        @return: The output dictionary, or None if no output was set
        """

        with self._lock:
            return self.outputs.get(job_id)

    def pending(self) -> bool:
        """Check whether any jobs still need to be run.

        @return: True if there are pending jobs, False otherwise
        """

        with self._lock:
            return any(entry.state == JobState.PENDING for entry in self.jobs.values())

    def runnable(self) -> Iterator[tuple[str, "Job"]]:
        """Yield all runnable jobs from the registry.

        @return: An iterator of (job ID, job) tuples for runnable jobs
        """

        with self._lock:
            runnable_list = []

            for job_id, entry in self.jobs.items():
                # Must be in pending to be checked
                if entry.state != JobState.PENDING:
                    continue

                job = entry.job
                status = job.ready()

                if status == DependencyState.SATISFIED:
                    runnable_list.append((job_id, job))
                elif status == DependencyState.IMPOSSIBLE:
                    # If any dependency is impossible, we can no longer run this job
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
