"""Defines contextual requests and their default Zahir providers."""

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import ClassVar, LiteralString

from orbis import BindingMap, Coeffect


@dataclass(frozen=True)
class CurrentTime(Coeffect[datetime]):
    """Requests the worker's current UTC wall-clock time."""

    tag: ClassVar[LiteralString] = "current_time"


def provide_current_time(_coeffect: CurrentTime) -> datetime:
    """Provide the current UTC wall-clock time."""

    return datetime.now(tz=UTC)


def build_default_providers() -> BindingMap:
    """Build the contextual providers available on every worker."""

    return {CurrentTime.tag: provide_current_time}


def build_providers(overrides: BindingMap | None = None) -> BindingMap:
    """Build worker providers with caller overrides applied last."""

    return {**build_default_providers(), **(overrides or {})}
