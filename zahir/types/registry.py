"""Registry abstract types.

Contains the JobRegistry and EventRegistry ABCs, plus the
JobInformation and JobTimingInformation data classes used
for querying registry state.
"""

from abc import ABC, abstractmethod
from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
import multiprocessing
from typing import TYPE_CHECKING, Any

from zahir.types.job_state import JobState

if TYPE_CHECKING:
    from zahir.types.context import Context
    from zahir.types.job import JobInstance

from zahir.events import ZahirEvent


@dataclass
class JobInformation:
    """Holds information about a job stored in the job registry."""

    # The job's ID
    job_id: str
    # The job instance
    job: "JobInstance"
    # The job's current state
    state: JobState
    # The output of the job, if there is one
    output: Mapping[str, Any] | None

    # When did the job start?
    started_at: datetime | None = None
    # When did the job complete?
    completed_at: datetime | None = None
    # The workflow ID this job belongs to
    workflow_id: str | None = None


@dataclass
class JobTimingInformation:
    """Timing information for a job."""

    started_at: datetime | None
    recovery_started_at: datetime | None
    completed_at: datetime | None

    def time_since_started(self) -> float | None:
        """Get the time since the job started in seconds."""

        if self.started_at is None:
            return None

        now = datetime.now(tz=UTC)
        delta = now - self.started_at
        return delta.total_seconds()

    def time_since_recovery_started(self) -> float | None:
        """Get the time since the job recovery started in seconds."""

        if self.recovery_started_at is None:
            return None

        now = datetime.now(tz=UTC)
        delta = now - self.recovery_started_at
        return delta.total_seconds()


class JobRegistry(ABC):
    """Keeps track of jobs to be run."""

    @abstractmethod
    def init(self, worker_id: str) -> None:
        """Initialise the job registry for use by a particular worker.

        @param worker_id: The ID of the worker using this registry
        """

        raise NotImplementedError

    @abstractmethod
    def add(self, context: "Context", job: "JobInstance", output_queue, workflow_id: str) -> str:
        """
        Register a job with the job registry, returning a job ID.

        This method MUST ensure that each job_id is registered at most once:
        - If a job with the same job_id already exists in the registry, this method MUST raise an exception.
        - This ensures that each job is run at most once per registry instance.

        @param job: The job to register
        @param workflow_id: The ID of the workflow this job belongs to
        @return: The job ID assigned to the job
        @raises ValueError: If the job_id is already registered
        """

        raise NotImplementedError

    @abstractmethod
    def is_active(self, workflow_id: str | None = None) -> bool:
        """Return True if any jobs are in a non-terminal state

        @param workflow_id: Optional workflow ID to filter by. If None, checks all workflows.
        @return: True if there are any active jobs (optionally for the specified workflow)
        """

        raise NotImplementedError

    @abstractmethod
    def is_finished(self, job_id: str) -> bool:
        """Is the job in a terminal state?"""

        raise NotImplementedError

    @abstractmethod
    def get_state(self, job_id: str) -> JobState:
        """Get the state of a job by ID"""

        raise NotImplementedError

    @abstractmethod
    def get_job_timing(self, job_id: str) -> "JobTimingInformation":
        """Get timing information for a job by ID"""

        raise NotImplementedError

    @abstractmethod
    def set_state(
        self,
        context: "Context",
        job_id: str,
        job_type: str,
        workflow_id: str,
        output_queue: multiprocessing.Queue,
        state: JobState,
        recovery: bool = False,
        error: Exception | None = None,
        pid: int | None = None,
    ) -> str:
        """Set the state of a job by ID.

        @param pid: Optional process ID of the worker executing the job. Used for events like JobStartedEvent.
        """

        raise NotImplementedError

    @abstractmethod
    def get_output(self, job_id: str, recovery: bool = False) -> Mapping | None:
        """Retrieve the output of a completed job

        @param job_id: The ID of the job
        @return: The output dictionary, or None if no output was set
        """

        raise NotImplementedError

    @abstractmethod
    def set_output(
        self,
        context: "Context",
        job_id: str,
        job_type: str,
        workflow_id: str,
        output_queue,
        output: Mapping,
        recovery: bool = False,
    ) -> None:
        """Store the output of a completed job

        @param job_id: The ID of the job
        @param job_type: The type of the job
        @param output: The output dictionary produced by the job
        @param recovery: Whether this output is from a recovery job
        """

        raise NotImplementedError

    @abstractmethod
    def get_errors(self, job_id: str, recovery: bool = False) -> list[Exception]:
        """Retrieve the errors associated with a job. A job can have multiple
        errors potentially (precheck error, then recovery error).

        @param job_id: The ID of the job
        @return: A list of errors
        """

        raise NotImplementedError

    @abstractmethod
    def get_active_workflow_ids(self) -> list[str]:
        """Get a list of all workflow IDs that have active jobs.

        @return: List of workflow IDs with active jobs
        """
        raise NotImplementedError

    @abstractmethod
    def jobs(
        self, context: "Context", state: JobState | None = None, workflow_id: str | None = None
    ) -> Iterator[JobInformation]:
        """Get an iterator of all jobs with their information.

        @param context: The context containing scope and registries for deserialisation
        @param state: Optional state to filter jobs by
        @param workflow_id: Optional workflow ID to filter jobs by
        @return: An iterator of JobInformation objects
        """

        raise NotImplementedError

    @abstractmethod
    def set_claim(self, job_id: str, worker_id: str) -> bool:
        """Set a claim for a job by a worker.

        Used by the overseer to mark a job as being processed by a specific worker.

        @param job_id: The ID of the job to claim
        @param worker_id: The ID of the worker claiming the job
        @return: True if the claim was set successfully
        """

        raise NotImplementedError

    @abstractmethod
    def get_workflow_duration(self, workflow_id: str | None = None) -> float | None:
        """Get the duration of the workflow in seconds.

        @param workflow_id: Optional workflow ID to filter by. If None, computes duration for all workflows.
        @return: The duration in seconds, or None if workflow hasn't completed
        """

        raise NotImplementedError

    @abstractmethod
    def on_startup(self) -> None:
        """Hook called when the job registry is started up."""

        raise NotImplementedError

    @abstractmethod
    def close(self) -> None:
        """Close the job registry."""

        raise NotImplementedError


class EventRegistry(ABC):
    """Keeps track of events occurring during workflow execution."""

    @abstractmethod
    def register(self, event: "ZahirEvent") -> None:
        """Register an event in the event registry."""

        raise NotImplementedError
