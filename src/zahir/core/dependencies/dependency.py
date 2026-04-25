# Polling combinator that drives condition generators, handling retries, timeouts, and Left/Right signalling.
from collections.abc import Callable, Generator
from datetime import UTC, datetime, timedelta
from typing import Any

from tertius import EEmit, ESleep

from zahir.core.constants import DEPENDENCY_DELAY_MS, DependencyState as SS
from zahir.core.zahir_types import DependencyResult, Satisfied
from zahir.core.exceptions import ImpossibleError


def check(
    condition_fn: Callable[[], Generator],
    label: str = "check",
) -> Generator[Any, Any, DependencyResult]:
    """Evaluate condition_fn once; return satisfied or impossible without retrying.

    condition_fn must return a generator that:
    - yields any effects it needs
    - returns True or (True, metadata) when the condition is met
    - returns False when not met
    - raises ImpossibleError when the condition can never be met
    """
    try:
        outcome = yield from condition_fn()
        satisfied, metadata = (
            outcome if isinstance(outcome, tuple) else (outcome, None)
        )
        if satisfied:
            result: DependencyResult = (SS.SATISFIED, metadata)
            yield EEmit(result)
            return result
    except ImpossibleError as exc:
        result = (SS.IMPOSSIBLE, str(exc))
        yield EEmit(result)
        return result

    result = (SS.IMPOSSIBLE, f"{label} condition not met")
    yield EEmit(result)
    return result


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
                SS.IMPOSSIBLE,
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
                result = (SS.SATISFIED, metadata)
                yield EEmit(result)
                return result
        except ImpossibleError as exc:
            result = (SS.IMPOSSIBLE, str(exc))
            yield EEmit(result)
            return result

        yield ESleep(ms=poll_ms)
