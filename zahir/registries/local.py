"""Registry management for workflow execution."""

from dataclasses import dataclass
from threading import Lock
from typing import Iterator
from zahir.events import ZahirEvent, WorkflowOutputEvent
from zahir.types import (
    Context,
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
        self._outputs: dict[str, dict] = {}
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
            self._outputs[job_id] = output

    def get_output(self, job_id: str) -> dict | None:
        """Retrieve the output of a completed job

        @param job_id: The ID of the job
        @return: The output dictionary, or None if no output was set
        """

        with self._lock:
            return self._outputs.get(job_id)

    def pending(self) -> bool:
        """Check whether any jobs still need to be run.

        @return: True if there are pending jobs, False otherwise
        """

        with self._lock:
            return any(entry.state == JobState.PENDING for entry in self.jobs.values())

    def running(self, context: Context) -> Iterator[tuple[str, "Job"]]:
        """Get an iterator of currently running jobs.

        @param context: The context containing scope and registries (unused for in-memory registry)
        @return: An iterator of (job ID, job) tuples for running jobs
        """

        running_states = {
            JobState.RUNNING,
            JobState.RECOVERING,
        }

        running_list = []
        with self._lock:
            for job_id, entry in self.jobs.items():
                if entry.state in running_states:
                    running_list.append((job_id, entry.job))

        for job_id, job in running_list:
            yield job_id, job

    def outputs(self, workflow_id: str) -> Iterator["WorkflowOutputEvent"]:
        """Get workflow output event containing all job outputs.

        @param workflow_id: The ID of the workflow
        @return: An iterator yielding a WorkflowOutputEvent with all outputs
        """

        with self._lock:
            output_dict = self._outputs.copy()

        if output_dict:
            yield WorkflowOutputEvent(workflow_id, output_dict)

    def runnable(self, context: Context) -> Iterator[tuple[str, "Job"]]:
        """Yield all runnable jobs from the registry.

        @param context: The context containing scope and registries (unused for in-memory registry)
        @return: An iterator of (job ID, job) tuples for runnable jobs
        """

        # First, collect pending jobs without holding the lock during ready() checks
        pending_jobs = []
        with self._lock:
            for job_id, entry in self.jobs.items():
                if entry.state == JobState.PENDING:
                    pending_jobs.append((job_id, entry.job))

        # Check readiness outside the lock to avoid deadlock
        # (job.ready() may call back into this registry via dependencies)
        runnable_list = []
        impossible_list = []

        for job_id, job in pending_jobs:
            status = job.ready()

            if status == DependencyState.SATISFIED:
                runnable_list.append((job_id, job))
            elif status == DependencyState.IMPOSSIBLE:
                impossible_list.append(job_id)

        # Update impossible jobs with the lock
        if impossible_list:
            with self._lock:
                for job_id in impossible_list:
                    if job_id in self.jobs:
                        self.jobs[job_id].state = JobState.IMPOSSIBLE

        # Yield runnable jobs
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
