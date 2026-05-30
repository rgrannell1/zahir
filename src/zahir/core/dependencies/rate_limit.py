# Dependency that blocks until at least N seconds have passed since the last satisfied occurrence.
import os
import time
from collections.abc import Generator
from functools import partial
from typing import Any

from bookman.events import point
from tertius import EEmit, ESleep

from zahir.core.constants import DependencyTag
from zahir.core.dependencies.dependency import dependency
from zahir.core.effects import EAcquire, EGetState, ESetState
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
    Emits a WAITING point before each sleep so the progress bar can show the blocked state.
    """
    acquired = yield EAcquire(name=f'rate_limit:{name}', limit=1)
    if not acquired:
        return ('unsatisfied', {'name': name, 'reason': 'slot busy'})

    elapsed = 0.0
    while True:
        raw = yield EGetState(name=f'rate_limit:last_at:{name}')
        last_at = float(raw) if raw else 0.0
        elapsed = time.time() - last_at
        if elapsed >= min_seconds:
            break
        yield EEmit(_waiting_point(label))
        remaining_ms = max(1, int((min_seconds - elapsed) * 1000))
        yield ESleep(ms=remaining_ms)

    yield ESetState(name=f'rate_limit:last_at:{name}', value=str(time.time()))
    return ('satisfied', {'name': name, 'elapsed': elapsed})


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
