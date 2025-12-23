"""Core type definitions used throughout Zahir."""

from abc import ABC, abstractmethod
from enum import Enum
from typing import Generic, Iterator, Self, TypeVar

from zahir.events import ZahirEvent
from zahir.exception import DependencyMissingException

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
    def load(cls, data: dict) -> Self:
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
    def add(self, job: "Job") -> int:
        """Register a job with the job registry, returning a job ID"""

        raise NotImplementedError

    @abstractmethod
    def get_state(self, job_id: int) -> JobState:
        """Get the state of a job by ID"""

        raise NotImplementedError

    @abstractmethod
    def set_state(self, job_id: int, state: JobState) -> int:
        """Set the state of a job by ID"""

        raise NotImplementedError

    @abstractmethod
    def pending(self) -> bool:
        """Check whether any jobs still need to be run."""

        raise NotImplementedError

    @abstractmethod
    def runnable(self) -> Iterator[tuple[int, "Job"]]:
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
# ++++++++++++++++++++++ Job +++++++++++++++++++++++++++++++++++++
# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

ArgsType = TypeVar("ArgsType", bound=dict)
DependencyType = TypeVar("DependencyType", bound=Dependency)


class Job(ABC, Generic[ArgsType, DependencyType]):
    """Jobs can depend on other jobs."""

    input: ArgsType
    parent: Self | None = None

    # Upper-limit on how long the job should run for
    JOB_TIMEOUT: int | None = None

    # Upper-limit on how long the recovery should run for
    RECOVER_TIMEOUT: int | None = None

    # The dependencies on which the job depends
    dependencies: dict[str, DependencyType]

    @staticmethod
    def precheck(input: ArgsType) -> list[str]:
        """Check that the inputs are as desired before running the job.

        @param input: The input arguments to this particular job
        @return: A list of error messages, if any
        """

        return []

    def get_dependency(self, name: str) -> Dependency:
        """Get a particular dependency by name.

        @param name: The name of the dependency to get

        @return: The dependency object
        """

        try:
            return self.dependencies[name]
        except KeyError:
            raise DependencyMissingException(f"Dependency '{name}' not found")

    def ready(self) -> DependencyState:
        """Are all dependencies satisfied?

        @return True if all dependencies are satisfied, False otherwise
        """

        states = {dep.satisfied() for dep in self.dependencies.values()}
        if DependencyState.IMPOSSIBLE in states:
            return DependencyState.IMPOSSIBLE

        if DependencyState.UNSATISFIED in states:
            return DependencyState.UNSATISFIED

        return DependencyState.SATISFIED

    @abstractmethod
    def run(self) -> Iterator["Job"]:
        """Run the job itself. Unhandled exceptions will be caught
        by the workflow executor, and routed to the `recover` method.

        @return: An iterator of sub-jobs to run after this one
        """

        return iter([])

    def recover(self, err: Exception) -> Iterator["Job"]:
        """The job failed with an unhandled exception. The job
        can define a particular way of handling the exception.

        @param err: The exception that was raised

        @return: An iterator of jobs to run to recover from the error
        """

        return iter([])
