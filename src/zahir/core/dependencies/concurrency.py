from collections.abc import Generator
from typing import Any

from zahir.core.dependencies.dependency import DependencyResult, dependency
from zahir.core.effects import EAcquire


def _concurrency_condition(name: str, limit: int) -> Generator[Any, Any, Any]:
    """Returns (True, metadata) if the slot was acquired, False if the slot is full."""
    acquired = yield EAcquire(name=name, limit=limit)
    if acquired:
        return (True, {"name": name, "limit": limit})
    return False


def concurrency_dependency(
    name: str,
    limit: int,
    timeout_ms: int | None = None,
) -> Generator[Any, Any, DependencyResult]:
    return dependency(
        lambda: _concurrency_condition(name, limit),
        timeout_ms=timeout_ms,
        label=f"concurrency slot '{name}'",
    )
