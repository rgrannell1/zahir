"""Defines contextual requests and their default Zahir providers."""

import pathlib
import time
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import ClassVar, Literal, LiteralString

import psutil
from orbis import BindingMap, Coeffect

from zahir.core.combinators import build_handler_map
from zahir.core.commons.constants import CPU_SAMPLE_INTERVAL_S

type ResourceType = Literal["cpu", "memory"]


@dataclass(frozen=True)
class CurrentTime(Coeffect[datetime]):
    """Requests the worker's current UTC wall-clock time."""

    tag: ClassVar[LiteralString] = "current_time"


@dataclass(frozen=True)
class MonotonicTime(Coeffect[float]):
    """Requests the worker's current monotonic-clock value."""

    tag: ClassVar[LiteralString] = "monotonic_time"


@dataclass(frozen=True)
class FileExists(Coeffect[bool]):
    """Requests whether a path exists on the worker filesystem."""

    path: str
    tag: ClassVar[LiteralString] = "file_exists"


@dataclass(frozen=True)
class ResourceUsage(Coeffect[float]):
    """Requests the worker's current resource usage percentage."""

    resource: ResourceType
    tag: ClassVar[LiteralString] = "resource_usage"


def provide_current_time(_coeffect: CurrentTime) -> datetime:
    """Provide the current UTC wall-clock time."""

    return datetime.now(tz=UTC)


def provide_monotonic_time(_coeffect: MonotonicTime) -> float:
    """Provide the current monotonic-clock value."""

    return time.monotonic()


def provide_file_exists(coeffect: FileExists) -> bool:
    """Provide path existence from the worker filesystem."""

    return pathlib.Path(coeffect.path).exists()


def provide_resource_usage(coeffect: ResourceUsage) -> float:
    """Provide the current usage percentage for one worker resource."""

    if coeffect.resource == "cpu":
        return psutil.cpu_percent(interval=CPU_SAMPLE_INTERVAL_S)
    if coeffect.resource == "memory":
        return psutil.virtual_memory().percent
    raise ValueError(f"unsupported resource: {coeffect.resource}")


def build_default_providers() -> BindingMap:
    """Build the contextual providers available on every worker."""

    return {
        CurrentTime.tag: provide_current_time,
        MonotonicTime.tag: provide_monotonic_time,
        FileExists.tag: provide_file_exists,
        ResourceUsage.tag: provide_resource_usage,
    }


def build_providers(
    overrides: BindingMap | None = None,
    wrappers: Sequence = (),
) -> BindingMap:
    """Build wrapped worker providers with caller overrides applied last."""

    providers = {**build_default_providers(), **(overrides or {})}
    return build_handler_map(providers, wrappers)
