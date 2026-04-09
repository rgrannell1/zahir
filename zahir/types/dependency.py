"""Dependency base types.

Contains the abstract Dependency class, DependencyState enum, DependencyResult,
and DependencyData TypedDict. These are the foundation that all concrete
dependency implementations build on.

This module has no internal Zahir imports, breaking the circular
dependency chain.
"""

from abc import ABC, abstractmethod
from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum
from typing import TYPE_CHECKING, Any, Self, TypedDict

if TYPE_CHECKING:
    from zahir.types.context import Context


class DependencyState(StrEnum):
    """The state of a dependency."""

    SATISFIED = "satisfied"
    UNSATISFIED = "unsatisfied"
    IMPOSSIBLE = "impossible"


@dataclass(frozen=True)
class DependencyResult:
    """Result of checking whether a dependency is satisfied."""

    # the type of the dependency
    type: str

    # the actual state of the dependency
    state: DependencyState

    # a message describing the result
    message: str | None = None

    # additional metadata about the dependency
    metadata: Mapping[str, Any] | None = None

    # the results of any subdependencies
    subdependencies: list["DependencyResult"] | None = None


class DependencyData(TypedDict, total=False):
    """Base structure for serialised dependency data.

    Specific dependency types may add additional fields.
    """

    type: str


class Dependency(ABC):
    """A dependency, on which a job can depend"""

    DEFAULT_MESSAGE: dict[DependencyState, str] = {}

    def message(self, templates: "dict[DependencyState, str]") -> Self:
        self.message_override = templates
        return self

    def render_message(
        self,
        state: DependencyState,
        metadata: "Mapping[str, Any] | None",
    ) -> "str | None":
        """Render a message for the given state and metadata.

        Checks the instance override first, then falls back to ``DEFAULT_MESSAGE``
        for that state. Returns ``None`` if neither has a template for the state.
        """
        override: dict[DependencyState, str] | None = getattr(self, "message_override", None)
        template = override.get(state) if override else None

        if template is None:
            template = self.DEFAULT_MESSAGE.get(state)

        if template is None:
            return None
        try:
            return template.format(**(metadata or {}))
        except (KeyError, ValueError):
            return template

    @abstractmethod
    def satisfied(self) -> DependencyResult:
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


def serialise_message_override(dependency: Dependency) -> dict[str, str]:
    """Return the dependency's message override as a serialisable dict, or empty dict."""
    override: dict[DependencyState, str] | None = getattr(dependency, "message_override", None)
    if not override:
        return {}
    return {state.value: template for state, template in override.items()}


def restore_message_override(dependency: Dependency, data: Mapping[str, Any]) -> None:
    """Restore a message override onto a dependency from serialised data."""
    override_data = data.get("message_override")
    if isinstance(override_data, dict):
        dependency.message_override = {DependencyState(k): v for k, v in override_data.items()}


def propagate_message_override(source: Dependency, target: Dependency) -> None:
    """Copy the message override from one dependency instance to another."""
    override: dict[DependencyState, str] | None = getattr(source, "message_override", None)
    if override:
        target.message_override = override
