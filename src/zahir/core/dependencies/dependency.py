# Polling combinator: drives condition generators with retries, timeouts, Left/Right.
import time
from collections.abc import Callable, Generator
from typing import Any

from tertius import EEmit, ESleep

from zahir.core.commons.clock import monotonic_deadline
from zahir.core.commons.constants import DEPENDENCY_DELAY_MS, DependencyState
from zahir.core.commons.zahir_types import ConditionResult, DependencyResult
from zahir.core.telemetry import dependency_satisfied_event, dependency_waiting_event


def finish(label: str, result: DependencyResult) -> Generator[Any, Any, DependencyResult]:
    """Emit the terminal result and the satisfied/abandoned event, then return the result."""

    yield EEmit(result)
    yield EEmit(dependency_satisfied_event(label))
    return result


def check(
    condition_fn: Callable[[], Generator[Any, Any, ConditionResult]],
    label: str = "check",
) -> Generator[Any, Any, DependencyResult]:
    """Evaluate condition_fn once; return satisfied or impossible without retrying.

    condition_fn must return a generator that:
    - yields any effects it needs
    - returns a ConditionResult: (satisfied, metadata), (unsatisfied, metadata),
      or (impossible, metadata)
    Unsatisfied is mapped to impossible since there is no retry in check mode.
    """
    state, metadata = yield from condition_fn()
    if state == DependencyState.SATISFIED:
        result: DependencyResult = (DependencyState.SATISFIED, metadata)
    else:
        # unsatisfied maps to impossible: there is no retry in one-shot mode
        result = (DependencyState.IMPOSSIBLE, metadata)
    return (yield from finish(label, result))


def make_timeout_result(label: str, timeout_ms: int | None) -> DependencyResult:
    """Create an impossible result for a dependency timeout."""

    reason = f"{label} timed out after {timeout_ms}ms"
    return DependencyState.IMPOSSIBLE, {"reason": reason}


def dependency(
    condition_fn: Callable[[], Generator[Any, Any, ConditionResult]],
    timeout_ms: int | None = None,
    poll_ms: int = DEPENDENCY_DELAY_MS,
    label: str = "dependency",
) -> Generator[Any, Any, DependencyResult]:
    """Poll condition_fn until it returns satisfied or impossible, or times out.

    condition_fn must return a generator that:
    - yields any effects it needs (EAcquire, EGetState, ESleep, etc.)
    - returns a ConditionResult: (satisfied | unsatisfied | impossible, metadata)

    Unsatisfied causes a retry after poll_ms; satisfied or impossible terminates the loop.
    """
    timeout_at = monotonic_deadline(timeout_ms)

    while True:
        if timeout_at is not None and time.monotonic() >= timeout_at:
            result = make_timeout_result(label, timeout_ms)
            return (yield from finish(label, result))

        state, metadata = yield from condition_fn()

        if state != DependencyState.UNSATISFIED:
            return (yield from finish(label, (state, metadata)))

        yield EEmit(dependency_waiting_event(label))
        yield ESleep(ms=poll_ms)
