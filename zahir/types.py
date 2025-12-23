"""Core type definitions used throughout Zahir."""

from abc import ABC, abstractmethod
from typing import Generic, Iterator, TypeVar

from zahir.events import ZahirEvent
from zahir.exception import DependencyMissingException

# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
# ++++++++++++++++++++++ Dependency ++++++++++++++++++++++++++++++++
# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++


class Dependency(ABC):
    """A dependency, on which a job can depend"""

    @abstractmethod
    def satisfied(self) -> bool:
        """Is the dependency satisfied?"""

        raise NotImplementedError


# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
# ++++++++++++++++++++++ Job Registry +++++++++++++++++++++++++++++++
# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++


class JobRegistry(ABC):
    """Keeps track of jobs to be run."""

    @abstractmethod
    def add(self, job: "Job") -> int:
        """Register a job with the job registry, returning a job ID"""

        raise NotImplementedError

    @abstractmethod
    def complete(self, job_id: int) -> int:
        """Remove a job from the job registry by ID"""

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

    def ready(self) -> bool:
        """Are all dependencies satisfied?

        @return True if all dependencies are satisfied, False otherwise
        """

        return all(dep.satisfied() for dep in self.dependencies.values())

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
