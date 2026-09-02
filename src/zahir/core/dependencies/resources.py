"""Dependency that waits until CPU or memory usage is within a given threshold."""

from collections.abc import Generator
from functools import partial
from typing import Any

from zahir.core.coeffects import ResourceType, ResourceUsage, provide_resource_usage
from zahir.core.commons.constants import DependencyState
from zahir.core.commons.zahir_types import ConditionResult, DependencyResult
from zahir.core.dependencies.dependency import dependency


def classify_resource_usage(
    resource: ResourceType,
    max_percent: float,
    usage_percent: float,
) -> ConditionResult:
    """Classify one resource usage observation against its limit."""

    metadata = {"resource": resource, "max_percent": max_percent}
    if usage_percent <= max_percent:
        return (DependencyState.SATISFIED, metadata)
    return (DependencyState.UNSATISFIED, metadata)


def resource_condition(
    resource: ResourceType,
    max_percent: float,
) -> ConditionResult:
    """Returns satisfied if resource usage is within the limit, unsatisfied otherwise."""
    usage_percent = provide_resource_usage(ResourceUsage(resource))
    return classify_resource_usage(resource, max_percent, usage_percent)


def request_resource_condition(
    resource: ResourceType,
    max_percent: float,
) -> Generator[Any, Any, ConditionResult]:
    """Classify resource usage supplied by the worker context."""

    usage_percent = yield ResourceUsage(resource)
    return classify_resource_usage(resource, max_percent, usage_percent)


def resource_dependency(
    resource: ResourceType,
    max_percent: float,
    timeout: float | None = None,
) -> Generator[Any, Any, DependencyResult]:
    timeout_ms = int(timeout * 1000) if timeout is not None else None

    return dependency(
        partial(request_resource_condition, resource, max_percent),
        timeout_ms=timeout_ms,
        label=f"{resource} resource",
    )
