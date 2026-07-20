# Dependency that waits until a named concurrency slot is available.
from collections.abc import Generator
from functools import partial
from typing import Any

from zahir.core.commons.constants import DependencyState
from zahir.core.commons.zahir_types import ConditionResult, DependencyResult
from zahir.core.dependencies.dependency import dependency
from zahir.core.effects import EAcquire


def concurrency_condition(name: str, limit: int) -> Generator[Any, Any, ConditionResult]:
    """Returns satisfied if the slot was acquired, unsatisfied if the slot is full."""
    acquired = yield EAcquire(name=name, limit=limit)
    if acquired:
        return (DependencyState.SATISFIED, {"name": name, "limit": limit})
    return (DependencyState.UNSATISFIED, {"name": name, "limit": limit})


def concurrency_dependency(
    name: str,
    limit: int,
    timeout_ms: int | None = None,
) -> Generator[Any, Any, DependencyResult]:
    return dependency(
        partial(concurrency_condition, name, limit),
        timeout_ms=timeout_ms,
        label=f"concurrency slot '{name}'",
    )
