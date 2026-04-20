from collections.abc import Generator

from zahir.core.constants import IMPOSSIBLE, SATISFIED
from zahir.core.dependencies.dependency import ImpossibleError, dependency
from zahir.core.effects import EGetSemaphore, EImpossible, ESatisfied


def _semaphore_condition(name: str) -> Generator:
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
) -> Generator[EGetSemaphore | ESatisfied | EImpossible, str | None, ESatisfied | EImpossible]:
    return dependency(
        lambda: _semaphore_condition(name),
        timeout_ms=timeout_ms,
        label=f"semaphore '{name}'",
    )
