# Polling combinator: drives condition generators with retries, timeouts, Left/Right.
import os
import time
from collections.abc import Callable, Generator
from typing import Any

from bookman.events import point
from tertius import EEmit, ESleep

from zahir.core.clock import monotonic_deadline
from zahir.core.constants import DEPENDENCY_DELAY_MS, DependencyState, DependencyTag
from zahir.core.zahir_types import ConditionResult, DependencyResult


def _waiting_event(label: str) -> object:
    data = {
        "tag": [DependencyTag.WAITING],
        "pid": [str(os.getpid())],
        "dep": [label],
    }
    return point(data, at=time.time())


def _done_event(label: str) -> object:
    data = {
        "tag": [DependencyTag.SATISFIED],
        "pid": [str(os.getpid())],
        "dep": [label],
    }
    return point(data, at=time.time())


def _finish(label: str, result: DependencyResult) -> Generator[Any, Any, DependencyResult]:
    """Emit the terminal result and the done event, then return the result."""

    yield EEmit(result)
    yield EEmit(_done_event(label))
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
        yield EEmit(result)
        return result
    if state == DependencyState.IMPOSSIBLE:
        result = (DependencyState.IMPOSSIBLE, metadata)
        yield EEmit(result)
        return result
    # UNSATISFIED: maps to impossible in one-shot mode
    result = (DependencyState.IMPOSSIBLE, metadata)
    yield EEmit(result)
    return result


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
            reason = f"{label} timed out after {timeout_ms}ms"
            return (yield from _finish(label, (DependencyState.IMPOSSIBLE, {"reason": reason})))

        state, metadata = yield from condition_fn()

        if state == DependencyState.SATISFIED:
            return (yield from _finish(label, (DependencyState.SATISFIED, metadata)))

        if state == DependencyState.IMPOSSIBLE:
            return (yield from _finish(label, (DependencyState.IMPOSSIBLE, metadata)))

        yield EEmit(_waiting_event(label))
        yield ESleep(ms=poll_ms)
