from typing import Self
from zahir.types import Scope, Job, Dependency


class LocalScope(Scope):
    """A local translation layer between dependency / job names and
    their underlying Python classes."""

    def __init__(self) -> None:
        self.jobs: dict[str, type[Job]] = {}
        self.dependencies: dict[str, type[Dependency]] = {}

    def add_job_class(self, TaskClass: type[Job]) -> Self:
        self.jobs[TaskClass.__name__] = TaskClass
        return self

    def add_job_classes(self, TaskClasses: list[type[Job]]) -> Self:
        for TaskClass in TaskClasses:
            self.jobs[TaskClass.__name__] = TaskClass
        return self

    def get_task_class(self, type_name: str) -> type[Job]:
        return self.jobs[type_name]

    def add_dependency_class(self, DependencyClass: type[Dependency]) -> Self:
        self.dependencies[DependencyClass.__name__] = DependencyClass
        return self

    def get_dependency_class(self, type_name: str) -> type[Dependency]:
        return self.dependencies[type_name]
