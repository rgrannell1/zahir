from collections.abc import Generator

from zahir.core.dependencies.dependency import dependency
from zahir.core.effects import EAcquire, EImpossible, ESatisfied


def _concurrency_condition(name: str, limit: int) -> Generator:
    """Returns (True, metadata) if the slot was acquired, False if the slot is full."""
    acquired = yield EAcquire(name=name, limit=limit)
    if acquired:
        return (True, {"name": name, "limit": limit})
    return False


def concurrency_dependency(
    name: str,
    limit: int,
    timeout_ms: int | None = None,
) -> Generator[EAcquire | ESatisfied | EImpossible, bool | None, ESatisfied | EImpossible]:
    return dependency(
        lambda: _concurrency_condition(name, limit),
        timeout_ms=timeout_ms,
        label=f"concurrency slot '{name}'",
    )
