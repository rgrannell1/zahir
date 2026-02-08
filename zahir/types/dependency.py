"""Dependency base types.

Contains the abstract Dependency class, DependencyState enum, and
DependencyData TypedDict. These are the foundation that all concrete
dependency implementations build on.

This module has no internal Zahir imports, breaking the circular
dependency chain.
"""

from abc import ABC, abstractmethod
from collections.abc import Mapping
from enum import StrEnum
from typing import TYPE_CHECKING, Any, Self, TypedDict

if TYPE_CHECKING:
    from zahir.types.context import Context


class DependencyState(StrEnum):
    """The state of a dependency."""

    SATISFIED = "satisfied"
    UNSATISFIED = "unsatisfied"
    IMPOSSIBLE = "impossible"


class DependencyData(TypedDict, total=False):
    """Base structure for serialised dependency data.

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
    def save(self, context: "Context") -> Mapping[str, Any]:
        """Serialise the dependency to a dictionary.

        @param context: The context containing scope and registries
        @return: The serialised dependency data
        """
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def load(cls, context: "Context", data: Mapping[str, Any]) -> Self:
        raise NotImplementedError
