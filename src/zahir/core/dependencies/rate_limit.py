# Dependency that blocks until at least N seconds have passed since the last satisfied occurrence.
import os
import time
from collections.abc import Generator
from functools import partial
from typing import Any

from bookman.events import point
from tertius import EEmit, ESleep

from zahir.core.constants import DependencyState, DependencyTag
from zahir.core.dependencies.dependency import dependency
from zahir.core.effects import EAcquire, EGetState, EReleaseSlot, ESetState
from zahir.core.zahir_types import ConditionResult, DependencyResult


def _waiting_point(label: str) -> object:
    data = {
        "tag": [DependencyTag.WAITING],
        "pid": [str(os.getpid())],
        "dep": [label],
    }
    return point(data, at=time.time())


def rate_limit_condition(
    name: str,
    min_seconds: float,
    label: str,
) -> Generator[Any, Any, ConditionResult]:
    """Satisfied when at least min_seconds have elapsed since the last satisfaction.

    Uses EAcquire(limit=1) as a mutex so only one job passes the gate at a time.
    After acquiring, waits internally with ESleep rather than returning 'unsatisfied' —
    returning unsatisfied while holding the slot would cause a deadlock because the
    dependency retry loop would call EAcquire again and find the slot already taken.
    The mutex is released with EReleaseSlot once the timestamp is stamped, so the gate
    only serialises the gap check — not the whole job that passes through it.
    Emits a WAITING point before each sleep so the progress bar can show the blocked state.
    """
    acquired = yield EAcquire(name=f'rate_limit:{name}', limit=1)
    if not acquired:
        return (DependencyState.UNSATISFIED, {'name': name, 'reason': 'slot busy'})

    elapsed = yield from wait_for_gap(name, min_seconds, label)

    yield ESetState(name=f'rate_limit:last_at:{name}', value=str(time.time()))
    yield EReleaseSlot(name=f'rate_limit:{name}')
    return (DependencyState.SATISFIED, {'name': name, 'elapsed': elapsed})


def wait_for_gap(name: str, min_seconds: float, label: str) -> Generator[Any, Any, float]:
    """Sleep inside the gate until min_seconds have passed since the last stamp."""

    while True:
        raw = yield EGetState(name=f'rate_limit:last_at:{name}')
        last_at = float(raw) if raw else 0.0
        elapsed = time.time() - last_at
        if elapsed >= min_seconds:
            return elapsed
        yield EEmit(_waiting_point(label))
        remaining_ms = max(1, int((min_seconds - elapsed) * 1000))
        yield ESleep(ms=remaining_ms)


def rate_limit_dependency(
    name: str,
    min_seconds: float,
    timeout_ms: int | None = None,
) -> Generator[Any, Any, DependencyResult]:
    """Block until at least min_seconds have elapsed since the last occurrence of name."""
    label = f"rate_limit '{name}' ({min_seconds}s)"
    return dependency(
        partial(rate_limit_condition, name, min_seconds, label),
        timeout_ms=timeout_ms,
        label=label,
    )
