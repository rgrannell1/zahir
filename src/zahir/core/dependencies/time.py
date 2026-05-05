# Dependency that waits until the current time is within a given window.
from collections.abc import Generator
from datetime import UTC, datetime
from functools import partial
from typing import Any

from zahir.core.constants import DependencyState
from zahir.core.dependencies.dependency import check, dependency
from zahir.core.zahir_types import ConditionResult, DependencyResult


def time_condition(
    before: datetime | None,
    after: datetime | None,
) -> Generator[Any, Any, ConditionResult]:
    """Returns satisfied if now is within the time window, unsatisfied if after hasn't arrived, impossible if before has passed."""  # noqa: E501
    now = datetime.now(tz=UTC)

    if before is not None and now >= before:
        return (DependencyState.IMPOSSIBLE, {"reason": f"too late: now={now.isoformat()}, before={before.isoformat()}"})

    if after is not None and now < after:
        return (DependencyState.UNSATISFIED, {"after": after.isoformat()})

    return (DependencyState.SATISFIED, {})
    yield


def check_time_dependency(
    before: datetime | None = None,
    after: datetime | None = None,
) -> Generator[Any, Any, DependencyResult]:
    """Evaluate the time condition once; return satisfied or impossible without sleeping."""
    return check(partial(time_condition, before, after), label="time")


def time_dependency(
    before: datetime | None = None,
    after: datetime | None = None,
) -> Generator[Any, Any, DependencyResult]:
    return dependency(partial(time_condition, before, after), label="time")
