# Dependency that waits until a named semaphore reaches the satisfied state.
from collections.abc import Generator
from functools import partial
from typing import Any

from zahir.core.constants import DependencyState as SS
from zahir.core.dependencies.dependency import dependency
from zahir.core.zahir_types import DependencyResult
from zahir.core.effects import EGetSemaphore
from zahir.core.exceptions import ImpossibleError


def _semaphore_condition(name: str) -> Generator[Any, Any, Any]:
    """Returns (True, metadata) if satisfied, False if unsatisfied, raises ImpossibleError if impossible."""

    state = yield EGetSemaphore(name=name)
    if state == SS.IMPOSSIBLE:
        raise ImpossibleError(f"semaphore '{name}' aborted")

    if state == SS.SATISFIED:
        return (True, {"name": name})

    return False


def semaphore_dependency(
    name: str,
    timeout_ms: int | None = None,
) -> Generator[Any, Any, DependencyResult]:
    return dependency(
        partial(_semaphore_condition, name),
        timeout_ms=timeout_ms,
        label=f"semaphore '{name}'",
    )
