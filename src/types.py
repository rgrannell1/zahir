"""Core type definitions used throughout Zahir."""

from abc import ABC, abstractmethod
from typing import Generic, Iterator, TypeVar

from exception import DependencyMissingException

# +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
# ++++++++++++++++++++++ Dependency +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
# +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++


class Dependency(ABC):
    """A dependency, on which a task can depend"""

    @abstractmethod
    def satisfied(self) -> bool:
        """Is the dependency satisfied?"""

        raise NotImplementedError


# +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
# ++++++++++++++++++++++ Job Queue ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
# +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++


class JobQueue(ABC):
    """Keeps track of jobs to be run."""

    @abstractmethod
    def add(self, task: "Task") -> int:
        """Register a task with the job queue, returning a job ID"""

        raise NotImplementedError

    @abstractmethod
    def complete(self, job_id: int) -> int:
        """Remove a job from the job queue by ID"""

        raise NotImplementedError


# +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
# ++++++++++++++++++++++ Tasks ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
# +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

ArgsType = TypeVar("ArgsType", bound=dict)
DependencyType = TypeVar("DependencyType", bound=Dependency)


class Task(ABC, Generic[ArgsType, DependencyType]):
    """Tasks can depend on other jobs."""

    input: ArgsType

    # Upper-limit on how long the task should run for
    TASK_TIMEOUT: int | None = None

    # Upper-limit on how long the recovery should run for
    RECOVER_TIMEOUT: int | None = None

    # The dependencies on which the task depends
    dependencies: dict[str, DependencyType]

    @staticmethod
    def precheck(input: ArgsType) -> list[str]:
        """Check that the inputs are as desired before running the task.

        @param input: The input arguments to this particular task

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
    def run(self) -> Iterator["Task"]:
        """Run the task itself. Unhandled exceptions will be caught
        by the workflow executor, and routed to the `recover` method.

        @return: An iterator of sub-tasks to run after this one
        """

        return iter([])

    def recover(self, err: Exception) -> Iterator["Task"]:
        """The task failed with an unhandled exception. The task
        can define a particular way of handling the exception.

        @param err: The exception that was raised

        @return: An iterator of tasks to run to recover from the error
        """

        return iter([])
