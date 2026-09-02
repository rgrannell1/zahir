# Dependency that waits until the current time is within a given window.
"""Defines the dependency that waits for a wall-clock window."""

from collections.abc import Generator
from datetime import datetime
from functools import partial
from typing import Any

from zahir.core.coeffects import CurrentTime, provide_current_time
from zahir.core.commons.constants import DependencyState
from zahir.core.commons.zahir_types import ConditionResult, DependencyResult
from zahir.core.dependencies.dependency import dependency


def classify_time_window(
    current_time: datetime,
    before: datetime | None,
    after: datetime | None,
) -> ConditionResult:
    """Satisfied inside the window, unsatisfied before it opens, impossible after it closes."""

    if before is not None and current_time >= before:
        reason = f"too late: now={current_time.isoformat()}, before={before.isoformat()}"
        return (DependencyState.IMPOSSIBLE, {"reason": reason})

    if after is not None and current_time < after:
        return (DependencyState.UNSATISFIED, {"after": after.isoformat()})

    return (DependencyState.SATISFIED, {})


def time_condition(
    before: datetime | None,
    after: datetime | None,
) -> ConditionResult:
    """Evaluate a time window directly for compatibility with the public API."""

    current_time = provide_current_time(CurrentTime())
    return classify_time_window(current_time, before, after)


def request_time_condition(
    before: datetime | None,
    after: datetime | None,
) -> Generator[Any, Any, ConditionResult]:
    """Evaluate a time window with the current time supplied as context."""

    current_time = yield CurrentTime()
    return classify_time_window(current_time, before, after)


def time_dependency(
    before: datetime | None = None,
    after: datetime | None = None,
) -> Generator[Any, Any, DependencyResult]:
    return dependency(partial(request_time_condition, before, after), label="time")
