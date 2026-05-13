"""Dependency that waits until a named semaphore reaches the satisfied state"""

from collections.abc import Generator
from functools import partial
from typing import Any

from zahir.core.constants import DependencyState
from zahir.core.dependencies.dependency import check, dependency
from zahir.core.effects import EGetState
from zahir.core.zahir_types import ConditionResult, DependencyResult


def semaphore_condition(name: str) -> Generator[Any, Any, ConditionResult]:
    """Returns satisfied, unsatisfied, or impossible based on the semaphore state."""

    state = yield EGetState(name=name)

    if state == DependencyState.IMPOSSIBLE:
        return ("impossible", {"name": name})

    if state == DependencyState.SATISFIED:
        return ("satisfied", {"name": name})

    return ("unsatisfied", {"name": name})


def check_semaphore_dependency(
    name: str,
) -> Generator[Any, Any, DependencyResult]:
    """Evaluate the semaphore state once; return satisfied or impossible without retrying."""

    return check(
        partial(semaphore_condition, name),
        label=f"semaphore '{name}'",
    )


def semaphore_dependency(
    name: str,
    timeout_ms: int | None = None,
) -> Generator[Any, Any, DependencyResult]:
    return dependency(
        partial(semaphore_condition, name),
        timeout_ms=timeout_ms,
        label=f"semaphore '{name}'",
    )
