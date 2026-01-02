"""Core type definitions used throughout Zahir."""

from abc import ABC, abstractmethod
from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
import multiprocessing
from typing import (
    TYPE_CHECKING,
    Any,
    Self,
    TypedDict,
    TypeVar,
    cast,
)

from zahir.events import ZahirEvent
from zahir.utils.id_generator import generate_id

if TYPE_CHECKING:
    from zahir.dependencies.group import DependencyGroup
    from zahir.events import JobOutputEvent, WorkflowOutputEvent, ZahirCustomEvent

# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
# ++++++++++++++++++++++ Dependency ++++++++++++++++++++++++++++++++
# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++


class DependencyState(StrEnum):
    """The state of a dependency."""

    SATISFIED = "satisfied"
    UNSATISFIED = "unsatisfied"
    IMPOSSIBLE = "impossible"


class DependencyData(TypedDict, total=False):
    """Base structure for serialized dependency data.

    Specific dependency types may add additional fields.
    """

    type: str


class Dependency(ABC):
    """A dependency, on which a job can depend"""

    @abstractmethod
    def satisfied(self) -> DependencyState:
        """Is the dependency satisfied?"""

        raise NotImplementedError

    @abstractmethod
    def request_extension(self, extra_seconds: float) -> Self:
        """A dependency may, if it chooses, allow you to request that it's valid longer
        than initially defined. This applies to time-based dependencies; other dependencies
        can just return themselves unchanged. Dependencies do not have to honour extension
        requests (sometimes we want hard-stops).

        The main use of these extensions is to support retries and backoff for jobs that use
        time-dependencies for scheduling.
        """
        ...

    @abstractmethod
    def save(self) -> Mapping[str, Any]:
        """Serialize the dependency to a dictionary.

        @return: The serialized dependency data
        """
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def load(cls, context: "Context", data: Mapping[str, Any]) -> Self:
        raise NotImplementedError


# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
# ++++++++++++++++++++++ Job Registry +++++++++++++++++++++++++++++++
# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++


class JobState(StrEnum):
    """Track the state jobs can be in"""

    # Still to be run
    PENDING = "pending"

    # Dependencies have not yet been satisfied for this job
    BLOCKED = "blocked"

    # Dependencies can never be satisfied, so
    # this job is impossible to run
    IMPOSSIBLE = "impossible"

    # Ready to run
    READY = "ready"

    # Claimed, now paused
    PAUSED = "paused"

    # Input validations for the job failed
    PRECHECK_FAILED = "precheck_failed"

    # Currently running
    RUNNING = "running"

    # Completed successfully
    COMPLETED = "completed"

    # Recovery running
    RECOVERING = "recovering"

    # Recovered successfully
    RECOVERED = "recovered"

    # Execution timed out
    TIMED_OUT = "timed_out"

    # Recovery timed out
    RECOVERY_TIMED_OUT = "recovery_timed_out"

    # Even rollback failed; this job is irrecoverable
    IRRECOVERABLE = "irrecoverable"


# Terminal job-states
COMPLETED_JOB_STATES = {
    JobState.IMPOSSIBLE,
    JobState.PRECHECK_FAILED,
    JobState.COMPLETED,
    JobState.RECOVERED,
    JobState.TIMED_OUT,
    JobState.RECOVERY_TIMED_OUT,
    JobState.IRRECOVERABLE,
}


# Active, non-terminal job-states
ACTIVE_JOB_STATES = {JobState.PENDING, JobState.BLOCKED, JobState.READY, JobState.PAUSED, JobState.RECOVERING}


@dataclass
class JobInformation:
    """Holds information about a job stored in the job registry."""

    # The job's ID
    job_id: str
    # The job instance
    job: "Job"
    # The job's current state
    state: JobState
    # The output of the job, if there is one
    output: Mapping[str, Any] | None

    # When did the job start?
    started_at: datetime | None = None
    # When did the job complete?
    completed_at: datetime | None = None
    # How long did the job take to run?
    duration_seconds: float | None = None
    # How long did recovery take, if any?
    recovery_duration_seconds: float | None = None


class JobRegistry(ABC):
    """Keeps track of jobs to be run."""

    @abstractmethod
    def init(self, worker_id: str) -> None:
        """Initialize the job registry for use by a particular worker.

        @param worker_id: The ID of the worker using this registry
        """

        raise NotImplementedError

    @abstractmethod
    def add(self, job: "Job", output_queue) -> str:
        """
        Register a job with the job registry, returning a job ID.

        This method MUST ensure that each job_id is registered at most once:
        - If a job with the same job_id already exists in the registry, this method MUST raise an exception.
        - This ensures that each job is run at most once per registry instance.

        @param job: The job to register
        @return: The job ID assigned to the job
        @raises ValueError: If the job_id is already registered
        """

        raise NotImplementedError

    @abstractmethod
    def is_active(self) -> bool:
        """Return True if any jobs are in a non-terminal state"""

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
        job_id: str,
        workflow_id: str,
        output_queue: multiprocessing.Queue,
        state: JobState,
        error: BaseException | None = None,
    ) -> str:
        """Set the state of a job by ID."""

        raise NotImplementedError

    @abstractmethod
    def get_output(self, job_id: str) -> Mapping | None:
        """Retrieve the output of a completed job

        @param job_id: The ID of the job
        @return: The output dictionary, or None if no output was set
        """

        raise NotImplementedError

    @abstractmethod
    def set_output(self, job_id: str, workflow_id: str, output_queue, output: Mapping) -> None:
        """Store the output of a completed job

        @param job_id: The ID of the job
        @param output: The output dictionary produced by the job
        """

        raise NotImplementedError

    @abstractmethod
    def get_errors(self, job_id: str) -> list[BaseException]:
        """Retrieve the errors associated with a job. A job can have multiple
        errors potentially (precheck errror, then recovery error).

        @param job_id: The ID of the job
        @return: A list of errors
        """

        raise NotImplementedError

    @abstractmethod
    def jobs(self, context: "Context", state: JobState | None = None) -> Iterator[JobInformation]:
        """Get an iterator of all jobs with their information.

        @param context: The context containing scope and registries for deserialization
        @return: An iterator of JobInformation objects
        """

        raise NotImplementedError

    @abstractmethod
    def claim(self, context: "Context", worker_id: str) -> "Job | None":
        """Claim a pending job for execution.

        This method should atomically select a pending job, mark it as claimed,
        and return its ID and Job instance. If no pending jobs are available,
        it should return None.

        @return: A tuple of (job_id, Job) if a job was claimed, or None if no pending jobs are available.
        """

        raise NotImplementedError


# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
# ++++++++++++++++++++++ Event Registry ++++++++++++++++++++++++++++
# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++


class EventRegistry(ABC):
    """Keeps track of events occurring during workflow execution."""

    @abstractmethod
    def register(self, event: "ZahirEvent") -> None:
        """Register an event in the event registry."""

        raise NotImplementedError


# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
# ++++++++++++++++++++++ Job +++++++++++++++++++++++++++++++++++++++
# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

ArgsType = TypeVar("ArgsType", bound=dict)
OutputType = TypeVar("OutputType", bound=dict)


class JobOptionsData(TypedDict, total=False):
    """Serialized structure for job options."""

    # Upper-limit on how long the job should run for
    job_timeout: float | None
    # Upper-limit on how long the recovery should run for
    recover_timeout: float | None


class SerialisedJob(TypedDict):
    """Serialized representation of a Job"""

    type: str

    # Unique job identifier
    job_id: str

    # Optional parent job identifier
    parent_id: str | None

    # The input parameters to the job. Must be JSON-serialisable
    input: Mapping[str, Any]

    # The serialised dependencies for the job
    dependencies: Mapping[str, Any]

    # The serialised options for the job
    options: JobOptionsData | None


class JobOptions:
    """General purpose options for a job"""

    # Upper-limit on how long the job should run for
    job_timeout: float | None = None

    # Upper-limit on how long the recovery should run for
    recover_timeout: float | None = None

    def __init__(self, job_timeout: float | None = None, recover_timeout: float | None = None) -> None:
        self.job_timeout = job_timeout
        self.recover_timeout = recover_timeout

    def save(self) -> JobOptionsData:
        """Serialize the job options to a dictionary.

        @return: The serialized options
        """
        return {
            "job_timeout": self.job_timeout,
            "recover_timeout": self.recover_timeout,
        }

    @classmethod
    def load(cls, data: JobOptionsData) -> "JobOptions":
        """Deserialize the job options from a dictionary.

        @param data: The serialized options data
        @return: The deserialized options
        """
        options = cls()
        options.job_timeout = data.get("job_timeout")
        options.recover_timeout = data.get("recover_timeout")
        return options


class Job[ArgsType, OutputType](ABC):
    """Jobs can depend on other jobs."""

    job_options: JobOptions | None

    # Optional parent job ID for traceability
    parent_id: str | None

    # Unique identifier for this job instance
    job_id: str

    # The input that the run function actually uses
    input: ArgsType

    # The dependencies on which the job depends
    dependencies: "DependencyGroup"

    def __init__(
        self,
        input: ArgsType,
        dependencies: "Mapping[str, Dependency | list[Dependency]] | DependencyGroup",
        options: JobOptions | None = None,
        job_id: str | None = None,
        parent_id: str | None = None,
    ) -> None:
        # Circular dependency fun, love you Python
        from zahir.dependencies.group import DependencyGroup  # TO DO lint disable

        self.parent_id = parent_id
        self.job_id = job_id if job_id is not None else generate_id(3)
        self.input = input
        self.dependencies = dependencies if isinstance(dependencies, DependencyGroup) else DependencyGroup(dependencies)
        self.job_options = options

    @staticmethod
    def precheck(input: ArgsType) -> list[str]:
        """Check that the inputs are as desired before running the job.

        @param input: The input arguments to this particular job
        @return: A list of error messages, if any
        """

        return []

    def ready(self) -> DependencyState:
        """Are all dependencies satisfied?

        @return: The state of the dependencies
        """

        return self.dependencies.satisfied()

    @classmethod
    @abstractmethod
    def run(
        cls, context: "Context", input: ArgsType, dependencies: "DependencyGroup"
    ) -> Iterator["Job | JobOutputEvent | WorkflowOutputEvent | ZahirCustomEvent"]:
        """Run the job itself. Unhandled exceptions will be caught
        by the workflow executor, and routed to the `recover` method.

        @param context: The context containing scope and registries
        @param input: The input arguments to this job
        @param dependencies: The dependencies for this job
        @return: An iterator of sub-jobs, JobOutputEvent, or WorkflowOutputEvent. When a JobOutputEvent is yielded, it becomes the job's output and no further items are processed. WorkflowOutputEvent can be yielded to emit workflow-level outputs.
        """

        # These are class-methods to avoid self-dependencies that will impact
        # serialisation.

        return iter([])

    @classmethod
    def recover(
        cls,
        context: "Context",
        input: ArgsType,
        dependencies: "DependencyGroup",
        err: Exception,
    ) -> Iterator["Job | JobOutputEvent | WorkflowOutputEvent | ZahirCustomEvent"]:
        """The job failed with an unhandled exception. The job
        can define a particular way of handling the exception.

        @param context: The context containing scope and registries
        @param input: The input arguments to this job
        @param dependencies: The dependencies for this job
        @param err: The exception that was raised

        @return: An iterator of recovery jobs, JobOutputEvent, or WorkflowOutputEvent. When a JobOutputEvent is yielded, it becomes the job's output and no further items are processed.
         WorkflowOutputEvent can be yielded to emit workflow-level outputs.
        """

        raise err
        yield

    def save(self) -> SerialisedJob:
        """Serialize the job to a dictionary.

        @return: The serialized job
        """

        return {
            "type": self.__class__.__name__,
            "job_id": self.job_id,
            "parent_id": self.parent_id,
            "input": cast(Mapping[str, Any], self.input),
            "dependencies": self.dependencies.save(),
            "options": self.job_options.save() if self.job_options else None,
        }

    @classmethod
    def load(cls, context: "Context", data: SerialisedJob) -> "Job[Any, Any]":
        """Deserialize the job from a dictionary.

        @param context: The context containing scope and registries
        @param data: The serialized job data

        @return: The deserialized job
        """

        job_type = data["type"]
        job_class = context.scope.get_job_class(job_type)
        from zahir.dependencies.group import DependencyGroup  # TO DO lint disable

        dependencies = data["dependencies"]

        return job_class(
            input=data["input"],
            dependencies=DependencyGroup.load(context, dependencies),
            options=JobOptions.load(data["options"]) if data["options"] else None,
            job_id=data["job_id"],
            parent_id=data.get("parent_id"),
        )

    def request_extension(self, extra_seconds: float) -> Self:
        """Request that all time-based dependencies be extended by the given number of seconds.

        @param extra_seconds: The number of extra seconds to request
        @return: A new Job instance with extended dependencies
        """

        extended_dependencies = self.dependencies.request_extension(extra_seconds)
        return self.__class__(
            input=self.input,
            dependencies=extended_dependencies,
            options=self.job_options,
            parent_id=self.job_id,
        )

    def copy(self) -> Self:
        """Create a copy of this job instance.

        @return: A new Job instance with the same properties. The job-id will differ.
        """

        return self.__class__(
            input=self.input,
            dependencies=self.dependencies,
            options=self.job_options,
            parent_id=self.job_id,
        )


# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
# ++++++++++++++++++++++ Scope +++++++++++++++++++++++++++++++++++++
# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++


class Scope(ABC):
    """We persist serialised dependency and job information to
    various stores. These ultimately need to be deserialized back into
    their correct Python classes.

    So, we'll explicitly register jobs and dependencies with a scope, to
    aid with this translation. The alternatives is decorators registering
    tasks at import-time, which is too side-effectful. Explicit > implicit.
    """

    @abstractmethod
    def add_job_class(self, job_class: type["Job"]) -> Self:
        """Add a job class to the scope."""
        ...

    @abstractmethod
    def add_job_classes(self, job_classes: list[type["Job"]]) -> Self:
        """Add multiple job classes to the scope."""
        ...

    @abstractmethod
    def get_job_class(self, type_name: str) -> type["Job"]:
        """Get a job class by its type name."""
        ...

    @abstractmethod
    def add_dependency_class(self, dependency_class: type[Dependency]) -> Self:
        """Add a dependency class to the scope."""
        ...

    @abstractmethod
    def add_dependency_classes(self, dependency_classes: list[type[Dependency]]) -> Self:
        """Add multiple dependency classes to the scope."""
        ...

    @abstractmethod
    def get_dependency_class(self, type_name: str) -> type[Dependency]:
        """Get a dependency class by its type name."""
        ...


# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
# ++++++++++++++++++++++ Context +++++++++++++++++++++++++++++++++++
# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++


@dataclass
class Context:
    """Context such as scope, the job registry, and event registry
    needs to be communicated to Jobs and Dependencies.
    """

    # The scope containing registered job and dependency classes
    scope: Scope
    # Keep track of jobs
    job_registry: JobRegistry
