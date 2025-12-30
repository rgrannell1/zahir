from typing import Self

from zahir.base_types import Dependency, Job, Scope
from zahir.exception import DependencyNotInScopeError, JobNotInScopeError


class LocalScope(Scope):
    """A local translation layer between dependency / job names and
    their underlying Python classes."""

    def __init__(self, jobs: list[type[Job]] = [], dependencies: list[type[Dependency]] = []) -> None:
        self.jobs: dict[str, type[Job]] = {}
        self.dependencies: dict[str, type[Dependency]] = {}

        self.add_job_classes(jobs)
        self.add_dependency_classes(dependencies)

    def add_job_class(self, job_class: type[Job]) -> Self:
        """Add a job class to the scope.

        @param job_class: The job class to add.
        """

        self.jobs[job_class.__name__] = job_class
        return self

    def add_job_classes(self, job_classes: list[type[Job]]) -> Self:
        """Add multiple job classes to the scope.

        @param job_classes: The job classes to add.
        """

        for job_class in job_classes:
            self.jobs[job_class.__name__] = job_class
        return self

    def get_job_class(self, type_name: str) -> type[Job]:
        """Get a job class from the scope by name.

        @param type_name: The name of the job class to get.
        """

        if type_name not in self.jobs:
            raise JobNotInScopeError(f"Job class '{type_name}' not found in scope. Did you register it?")

        return self.jobs[type_name]

    def add_dependency_class(self, dependency_class: type[Dependency]) -> Self:
        """Add a dependency class to the scope.

        @param dependency_class: The dependency class to add.
        """

        self.dependencies[dependency_class.__name__] = dependency_class
        return self

    def add_dependency_classes(self, dependency_classes: list[type[Dependency]]) -> Self:
        """Add multiple dependency classes to the scope.

        @param dependency_classes: The dependency classes to add.
        """

        for dependency_class in dependency_classes:
            self.dependencies[dependency_class.__name__] = dependency_class
        return self

    def get_dependency_class(self, type_name: str) -> type[Dependency]:
        """Get a dependency class from the scope by name.

        @param type_name: The name of the dependency class to get.
        """

        if type_name not in self.dependencies:
            raise DependencyNotInScopeError(f"Dependency class '{type_name}' not found in scope. Did you register it?")

        return self.dependencies[type_name]
