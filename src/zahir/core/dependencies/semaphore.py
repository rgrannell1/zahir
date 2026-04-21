from collections.abc import Generator
from typing import Any

from zahir.core.constants import IMPOSSIBLE, SATISFIED
from zahir.core.dependencies.dependency import (
    DependencyResult,
    ImpossibleError,
    dependency,
)
from zahir.core.effects import EGetSemaphore


def _semaphore_condition(name: str) -> Generator[Any, Any, Any]:
    """Returns (True, metadata) if satisfied, False if unsatisfied, raises ImpossibleError if impossible."""
    state = yield EGetSemaphore(name=name)
    if state == IMPOSSIBLE:
        raise ImpossibleError(f"semaphore '{name}' aborted")
    if state == SATISFIED:
        return (True, {"name": name})
    return False


def semaphore_dependency(
    name: str,
    timeout_ms: int | None = None,
) -> Generator[Any, Any, DependencyResult]:
    return dependency(
        lambda: _semaphore_condition(name),
        timeout_ms=timeout_ms,
        label=f"semaphore '{name}'",
    )
