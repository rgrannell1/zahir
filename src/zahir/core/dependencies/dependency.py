from collections.abc import Callable, Generator
from datetime import UTC, datetime, timedelta
from typing import Any

from tertius import ESleep

from zahir.core.constants import DEPENDENCY_DELAY_MS
from zahir.core.effects import EImpossible, ESatisfied


class ImpossibleError(Exception):
    """Raised by a condition function to signal the dependency can never be satisfied."""


def dependency(
    condition_fn: Callable[[], Generator],
    timeout_ms: int | None = None,
    poll_ms: int = DEPENDENCY_DELAY_MS,
    label: str = "dependency",
) -> Generator[Any, Any, ESatisfied | EImpossible]:
    """Poll condition_fn until it returns True, raises ImpossibleError, or times out.

    condition_fn must return a generator that:
    - yields any effects it needs (EAcquire, EGetSemaphore, ESleep, etc.)
    - returns True when the condition is met
    - returns False when not yet (will retry after poll_ms)
    - raises ImpossibleError when the condition can never be met
    """
    timeout_at = (
        datetime.now(tz=UTC) + timedelta(milliseconds=timeout_ms)
        if timeout_ms is not None
        else None
    )

    while True:
        if timeout_at is not None and datetime.now(tz=UTC) >= timeout_at:
            event = EImpossible(reason=f"{label} timed out after {timeout_ms}ms")
            yield event
            return event

        try:
            result = yield from condition_fn()
            satisfied, metadata = result if isinstance(result, tuple) else (result, None)
            if satisfied:
                event = ESatisfied(metadata=metadata)
                yield event
                return event
        except ImpossibleError as exc:
            event = EImpossible(reason=str(exc))
            yield event
            return event

        yield ESleep(ms=poll_ms)
