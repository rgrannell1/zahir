"""Core type definitions used throughout Zahir."""

from abc import ABC, abstractmethod
from enum import Enum
from typing import Any, Generic, Iterator, Mapping, Self, TypedDict, TypeVar
from uuid import uuid4

from zahir.dependencies.group import DependencyGroup
from zahir.events import ZahirEvent

# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
# ++++++++++++++++++++++ Dependency ++++++++++++++++++++++++++++++++
# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++


class DependencyState(str, Enum):
    """The state of a dependency."""

    SATISFIED = "satisfied"
    UNSATISFIED = "unsatisfied"
    IMPOSSIBLE = "impossible"


class Dependency(ABC):
    """A dependency, on which a job can depend"""

    @abstractmethod
    def satisfied(self) -> DependencyState:
        """Is the dependency satisfied?"""

        raise NotImplementedError

    @abstractmethod
    def save(self) -> dict:
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def load(cls, context: 'Context', data: dict) -> Self:
        raise NotImplementedError


# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
# ++++++++++++++++++++++ Job Registry +++++++++++++++++++++++++++++++
# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++


class JobState(str, Enum):
    """Track the state jobs can be in"""

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


class JobRegistry(ABC):
    """Keeps track of jobs to be run."""

    @abstractmethod
    def add(self, job: "Job") -> str:
        """Register a job with the job registry, returning a job ID"""

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
    def pending(self) -> bool:
        """Check whether any jobs still need to be run."""

        raise NotImplementedError

    @abstractmethod
    def runnable(self) -> Iterator[tuple[str, "Job"]]:
        """Get an iterator of runnable jobs (ID, Job)"""

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
DependencyType = TypeVar("DependencyType", bound=Dependency)


class SerialisedJob(TypedDict):
    """Serialized representation of a Job"""

    type: str

    # Unique job identifier
    job_id: str

    # Optional parent job identifier
    parent_id: str | None

    # The input parameters to the job. Must be JSON-serialisable
    input: dict[str, Any]

    # The serialised dependencies for the job
    dependencies: dict[str, Any]

    # The serialised options for the job
    options: dict[str, Any] | None


class JobOptions:
    """General purpose options for a job"""

    # Upper-limit on how long the job should run for
    job_timeout: int | None = None

    # Upper-limit on how long the recovery should run for
    recover_timeout: int | None = None

    def save(self) -> dict[str, Any]:
        """Serialize the job options to a dictionary.

        @return: The serialized options
        """
        return {
            "job_timeout": self.job_timeout,
            "recover_timeout": self.recover_timeout,
        }

    @classmethod
    def load(cls, data: dict[str, Any]) -> "JobOptions":
        """Deserialize the job options from a dictionary.

        @param data: The serialized options data
        @return: The deserialized options
        """
        options = cls()
        options.job_timeout = data.get("job_timeout")
        options.recover_timeout = data.get("recover_timeout")
        return options


class Job(ABC, Generic[ArgsType, DependencyType]):
    """Jobs can depend on other jobs."""

    # Optional parent job ID for traceability
    parent_id: str | None

    # Unique identifier for this job instance
    job_id: str

    # The input that the run function actually uses
    input: ArgsType

    # The dependencies on which the job depends
    dependencies: DependencyGroup

    def __init__(
        self,
        input: ArgsType,
        dependencies: Mapping[str, DependencyType | list[DependencyType]],
        options: JobOptions | None = None,
    ) -> None:
        self.parent_id = None
        self.job_id = str(uuid4())
        self.input = input
        self.dependencies = DependencyGroup(dependencies)
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
        cls, context: "Context", input: ArgsType, dependencies: DependencyGroup
    ) -> Iterator["Job"]:
        """Run the job itself. Unhandled exceptions will be caught
        by the workflow executor, and routed to the `recover` method.

        @param context: The context containing scope and registries
        @param input: The input arguments to this job
        @param dependencies: The dependencies for this job
        @return: An iterator of sub-jobs to run after this one
        """

        # These are class-methods to avoid self-dependencies that will impact
        # serialisation.

        return iter([])

    @classmethod
    def recover(
        cls, context: "Context", input: ArgsType, dependencies: DependencyGroup, err: Exception
    ) -> Iterator["Job"]:
        """The job failed with an unhandled exception. The job
        can define a particular way of handling the exception.

        @param context: The context containing scope and registries
        @param input: The input arguments to this job
        @param dependencies: The dependencies for this job
        @param err: The exception that was raised
        @return: An iterator of jobs to run to recover from the error
        """

        # TODO yield to an error-reporter task.
        return iter([])

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
    def load(cls, context: 'Context', data: SerialisedJob) -> Self:
        """Deserialize the job from a dictionary.

        @param context: The context containing scope and registries
        @param data: The serialized job data

        @return: The deserialized job
        """

        # requires a scope lookup to find the correct class to construct things.

        raise NotImplementedError

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
    def add_task_class(self, TaskClass: type["Job"]) -> None:
        ...

    @abstractmethod
    def get_task_class(self, type_name: str) -> type["Job"]:
        ...

    @abstractmethod
    def add_dependency_class(self, DependencyClass: type[Dependency]) -> None:
        ...

    @abstractmethod
    def get_dependency_class(self, type_name: str) -> type[Dependency]:
        ...

# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
# ++++++++++++++++++++++ Context +++++++++++++++++++++++++++++++++++
# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

class Context:
    """Context such as scope, the job registry, and event registry
    needs to be communicated to Jobs and Dependencies.
    """

    scope: Scope
    job_registry: JobRegistry
    event_registry: EventRegistry

    def __init__(
        self,
        scope: Scope,
        job_registry: JobRegistry,
        event_registry: EventRegistry,
    ) -> None:
        self.scope = scope
        self.job_registry = job_registry
        self.event_registry = event_registry
