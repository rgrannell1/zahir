from collections.abc import Callable, Generator
from datetime import UTC, datetime, timedelta
from typing import Any, Literal

from tertius import EEmit, ESleep

from zahir.core.constants import DEPENDENCY_DELAY_MS
from zahir.core.exceptions import ImpossibleError

type Satisfied = tuple[Literal["satisfied"], dict | None]
type Impossible = tuple[Literal["impossible"], str]
type DependencyResult = Satisfied | Impossible


def dependency(
    condition_fn: Callable[[], Generator],
    timeout_ms: int | None = None,
    poll_ms: int = DEPENDENCY_DELAY_MS,
    label: str = "dependency",
) -> Generator[Any, Any, DependencyResult]:
    """Poll condition_fn until it returns True, raises ImpossibleError, or times out.

    condition_fn must return a generator that:
    - yields any effects it needs (EAcquire, EGetSemaphore, ESleep, etc.)
    - returns True or (True, metadata) when the condition is met
    - returns False when not yet met (will retry after poll_ms)
    - raises ImpossibleError when the condition can never be met
    """
    timeout_at = (
        datetime.now(tz=UTC) + timedelta(milliseconds=timeout_ms)
        if timeout_ms is not None
        else None
    )

    while True:
        if timeout_at is not None and datetime.now(tz=UTC) >= timeout_at:
            result: DependencyResult = (
                "impossible",
                f"{label} timed out after {timeout_ms}ms",
            )
            yield EEmit(result)
            return result

        try:
            outcome = yield from condition_fn()
            satisfied, metadata = (
                outcome if isinstance(outcome, tuple) else (outcome, None)
            )
            if satisfied:
                result = ("satisfied", metadata)
                yield EEmit(result)
                return result
        except ImpossibleError as exc:
            result = ("impossible", str(exc))
            yield EEmit(result)
            return result

        yield ESleep(ms=poll_ms)
