"""Scope abstract type.

Contains the Scope ABC which maps serialised type names back to
their Python classes, and the Transform type alias.
"""

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Self

from zahir.types.dependency import Dependency

if TYPE_CHECKING:
    from zahir.types.job import JobSpec
    from zahir.types.transform import Transform


class Scope(ABC):
    """We persist serialised dependency and job information to
    various stores. These ultimately need to be deserialised back into
    their correct Python classes.

    So, we'll explicitly register jobs and dependencies with a scope, to
    aid with this translation. The alternative is decorators registering
    tasks at import-time, which is too side-effectful. Explicit > implicit.
    """

    @abstractmethod
    def get_job_spec(self, type_name: str) -> "JobSpec": ...

    @abstractmethod
    def add_job_specs(self, specs: list["JobSpec"]) -> Self:
        """Add multiple job specs to the scope."""
        ...

    @abstractmethod
    def add_job_spec(self, spec: "JobSpec") -> Self:
        """Add a job spec"""
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

    @abstractmethod
    def get_transform(self, type_name: str) -> "Transform":
        """Get a transform function by its type name."""
        ...

    @abstractmethod
    def add_transform(self, type_name: str, transform: "Transform") -> Self:
        """Add a transform function to the scope."""
        ...
