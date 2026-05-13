# Polling combinator that drives condition generators, handling retries, timeouts, and Left/Right signalling.
import os
import time
from collections.abc import Callable, Generator
from datetime import UTC, datetime, timedelta
from typing import Any

from bookman.events import point
from tertius import EEmit, ESleep

from zahir.core.constants import DEPENDENCY_DELAY_MS, DependencyState, DependencyTag
from zahir.core.zahir_types import ConditionResult, DependencyResult


def _waiting_event(label: str) -> object:
    return point({"tag": [DependencyTag.WAITING], "pid": [str(os.getpid())], "dep": [label]}, at=time.time())


def _done_event(label: str) -> object:
    return point({"tag": [DependencyTag.SATISFIED], "pid": [str(os.getpid())], "dep": [label]}, at=time.time())


def check(
    condition_fn: Callable[[], Generator[Any, Any, ConditionResult]],
    label: str = "check",
) -> Generator[Any, Any, DependencyResult]:
    """Evaluate condition_fn once; return satisfied or impossible without retrying.

    condition_fn must return a generator that:
    - yields any effects it needs
    - returns a ConditionResult: (satisfied, metadata), (unsatisfied, metadata), or (impossible, metadata)
    Unsatisfied is mapped to impossible since there is no retry in check mode.
    """
    state, metadata = yield from condition_fn()
    if state == DependencyState.SATISFIED:
        result: DependencyResult = ("satisfied", metadata)
        yield EEmit(result)
        return result
    if state == DependencyState.IMPOSSIBLE:
        result = ("impossible", metadata)
        yield EEmit(result)
        return result
    # UNSATISFIED: maps to impossible in one-shot mode
    result = ("impossible", metadata)
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
    timeout_at = datetime.now(tz=UTC) + timedelta(milliseconds=timeout_ms) if timeout_ms is not None else None

    while True:
        if timeout_at is not None and datetime.now(tz=UTC) >= timeout_at:
            impossible: DependencyResult = ("impossible", {"reason": f"{label} timed out after {timeout_ms}ms"})
            yield EEmit(impossible)
            yield EEmit(_done_event(label))

            return impossible

        state, metadata = yield from condition_fn()

        if state == DependencyState.SATISFIED:
            satisfied: DependencyResult = ("satisfied", metadata)

            yield EEmit(satisfied)
            yield EEmit(_done_event(label))

            return satisfied

        if state == DependencyState.IMPOSSIBLE:
            impossible = ("impossible", metadata)

            yield EEmit(impossible)
            yield EEmit(_done_event(label))

            return impossible

        yield EEmit(_waiting_event(label))
        yield ESleep(ms=poll_ms)
