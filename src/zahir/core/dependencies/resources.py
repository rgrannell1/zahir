# Dependency that waits until CPU or memory usage is within a given threshold.

from collections.abc import Generator
from functools import partial
from typing import Any, Literal

import psutil

from zahir.core.constants import CPU_SAMPLE_INTERVAL_S
from zahir.core.dependencies.dependency import check, dependency
from zahir.core.zahir_types import DependencyResult

type ResourceType = Literal["cpu", "memory"]


def _get_usage(resource: ResourceType) -> float:
    match resource:
        case "cpu":
            return psutil.cpu_percent(interval=CPU_SAMPLE_INTERVAL_S)
        case "memory":
            return psutil.virtual_memory().percent


def _resource_condition(
    resource: ResourceType, max_percent: float
) -> Generator[Any, Any, Any]:
    """Returns (True, metadata) if resource usage is within the limit, False otherwise."""

    if _get_usage(resource) <= max_percent:
        return (True, {"resource": resource, "max_percent": max_percent})

    return False
    yield


def check_resource_dependency(
    resource: ResourceType,
    max_percent: float,
) -> Generator[Any, Any, DependencyResult]:
    """Evaluate resource usage once; return satisfied or impossible without retrying."""
    return check(
        partial(_resource_condition, resource, max_percent),
        label=f"{resource} resource",
    )


def resource_dependency(
    resource: ResourceType,
    max_percent: float,
    timeout: float | None = None,
) -> Generator[Any, Any, DependencyResult]:
    timeout_ms = int(timeout * 1000) if timeout is not None else None
    return dependency(
        partial(_resource_condition, resource, max_percent),
        timeout_ms=timeout_ms,
        label=f"{resource} resource",
    )
