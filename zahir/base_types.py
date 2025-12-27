"""Core type definitions used throughout Zahir."""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import (
    TYPE_CHECKING,
    Any,
    Generic,
    Iterator,
    Mapping,
    Self,
    TypedDict,
    TypeVar,
)

from zahir.events import ZahirEvent
from zahir.utils.id_generator import generate_id

if TYPE_CHECKING:
    from zahir.dependencies.group import DependencyGroup
    from zahir.events import JobOutputEvent, WorkflowOutputEvent, ZahirCustomEvent
    from zahir.logger import ZahirLogger

# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
# ++++++++++++++++++++++ Dependency ++++++++++++++++++++++++++++++++
# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++


class DependencyState(str, Enum):
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


class JobState(str, Enum):
    """Track the state jobs can be in"""

    # Job has been claimed by a worker, but not yet started
    CLAIMED = "claimed"

    # Still to be run
    PENDING = "pending"

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

    # Dependencies can never be satisfied, so
    # this job is impossible to run
    IMPOSSIBLE = "impossible"


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
    def add(self, job: "Job") -> str:
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
    def get_state(self, job_id: str) -> JobState:
        """Get the state of a job by ID"""

        raise NotImplementedError

    @abstractmethod
    def set_state(self, job_id: str, state: JobState) -> str:
        """Set the state of a job by ID"""

        raise NotImplementedError

    @abstractmethod
    def set_output(self, job_id: str, output: Mapping) -> None:
        """Store the output of a completed job

        @param job_id: The ID of the job
        @param output: The output dictionary produced by the job
        """

        raise NotImplementedError

    @abstractmethod
    def set_timing(
        self,
        job_id: str,
        started_at: datetime | None = None,
        completed_at: datetime | None = None,
        duration_seconds: float | None = None,
    ) -> None:
        """Store timing information for a job

        @param job_id: The ID of the job
        @param started_at: When the job started execution
        @param completed_at: When the job completed execution
        @param duration_seconds: How long the job took to execute
        """

        raise NotImplementedError

    @abstractmethod
    def set_recovery_duration(self, job_id: str, duration_seconds: float) -> None:
        """Store recovery duration for a job

        @param job_id: The ID of the job
        @param duration_seconds: How long the recovery took
        """

        raise NotImplementedError

    @abstractmethod
    def get_output(self, job_id: str) -> Mapping | None:
        """Retrieve the output of a completed job

        @param job_id: The ID of the job
        @return: The output dictionary, or None if no output was set
        """

        raise NotImplementedError

    @abstractmethod
    def pending(self) -> bool:
        """Check whether any jobs still need to be run."""

        raise NotImplementedError

    @abstractmethod
    def runnable(self, context: "Context") -> Iterator[tuple[str, "Job"]]:
        """Get an iterator of runnable jobs (ID, Job)

        @param context: The context containing scope and registries for deserialization
        """

        raise NotImplementedError

    @abstractmethod
    def running(self, context: "Context") -> Iterator[tuple[str, "Job"]]:
        """Get an iterator of currently running jobs (ID, Job).

        @param context: The context containing scope and registries for deserialization
        """

        raise NotImplementedError

    @abstractmethod
    def jobs(self, context: "Context") -> Iterator[JobInformation]:
        """Get an iterator of all jobs with their information.

        @param context: The context containing scope and registries for deserialization
        @return: An iterator of JobInformation objects
        """

        raise NotImplementedError

    @abstractmethod
    def outputs(self, workflow_id: str) -> Iterator["WorkflowOutputEvent"]:
        """Get workflow output event containing all job outputs.

        @param workflow_id: The ID of the workflow
        @return: An iterator yielding a WorkflowOutputEvent with all outputs
        """

        raise NotImplementedError

    @abstractmethod
    def claim(self, context: "Context") -> "Job | None":
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

    def __init__(
        self, job_timeout: float | None = None, recover_timeout: float | None = None
    ) -> None:
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


class Job(ABC, Generic[ArgsType, OutputType]):
    """Jobs can depend on other jobs."""

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
        dependencies: Mapping[str, Dependency | list[Dependency]] | "DependencyGroup",
        options: JobOptions | None = None,
        job_id: str | None = None,
        parent_id: str | None = None,
    ) -> None:
        # Import here to avoid circular dependency, this is so dumb.
        from zahir.dependencies.group import DependencyGroup

        self.parent_id = parent_id
        self.job_id = job_id if job_id is not None else generate_id(3)
        self.input = input
        self.dependencies = (
            DependencyGroup(dependencies)
            if isinstance(dependencies, dict)
            else dependencies
        )
        self.options = options

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

    def save(self) -> SerialisedJob:
        """Serialize the job to a dictionary.

        @return: The serialized job
        """

        return {
            "type": self.__class__.__name__,
            "job_id": self.job_id,
            "parent_id": self.parent_id,
            "input": self.input,
            "dependencies": self.dependencies.save(),
            "options": self.options.save() if self.options else None,
        }

    @classmethod
    def load(cls, context: "Context", data: SerialisedJob) -> "Job[Any, Any]":
        """Deserialize the job from a dictionary.

        @param context: The context containing scope and registries
        @param data: The serialized job data

        @return: The deserialized job
        """

        job_type = data["type"]
        JobClass = context.scope.get_job_class(job_type)
        from zahir.dependencies.group import DependencyGroup

        dependencies = data["dependencies"]

        job = JobClass(
            input=data["input"],
            dependencies=DependencyGroup.load(context, dependencies),
            options=JobOptions.load(data["options"]) if data["options"] else None,
            job_id=data["job_id"],
            parent_id=data.get("parent_id"),
        )

        return job


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
    def add_job_class(self, TaskClass: type["Job"]) -> Self:
        """Add a job class to the scope."""
        ...

    @abstractmethod
    def add_job_classes(self, TaskClasses: list[type["Job"]]) -> Self:
        """Add multiple job classes to the scope."""
        ...

    @abstractmethod
    def get_job_class(self, type_name: str) -> type["Job"]:
        """Get a job class by its type name."""
        ...

    @abstractmethod
    def add_dependency_class(self, DependencyClass: type[Dependency]) -> Self:
        """Add a dependency class to the scope."""
        ...

    @abstractmethod
    def add_dependency_classes(self, DependencyClasses: list[type[Dependency]]) -> Self:
        """Add multiple dependency classes to the scope."""
        ...

    @abstractmethod
    def get_dependency_class(self, type_name: str) -> type[Dependency]:
        """Get a dependency class by its type name."""
        ...


# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
# ++++++++++++++++++++++ Context +++++++++++++++++++++++++++++++++++
# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++


class Context:
    """Context such as scope, the job registry, and event registry
    needs to be communicated to Jobs and Dependencies.
    """

    # The scope containing registered job and dependency classes
    scope: Scope
    # Keep track of jobs
    job_registry: JobRegistry
    # Keep track of events
    event_registry: EventRegistry
    # Log the progress of the workflow
    logger: "ZahirLogger"

    def __init__(
        self,
        scope: Scope,
        job_registry: JobRegistry,
        event_registry: EventRegistry,
        logger: "ZahirLogger",
    ) -> None:
        self.scope = scope
        self.job_registry = job_registry
        self.event_registry = event_registry
        self.logger = logger


# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
# ++++++++++++++++++++++ Worker ++++++++++++++++++++++++++++++++++++
# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++


class Worker(ABC): ...
