# Dependency that waits until the current time is within a given window.
from collections.abc import Generator
from datetime import UTC, datetime
from functools import partial
from typing import Any

from zahir.core.commons.constants import DependencyState
from zahir.core.commons.zahir_types import ConditionResult, DependencyResult
from zahir.core.dependencies.dependency import dependency


def time_condition(
    before: datetime | None,
    after: datetime | None,
) -> Generator[Any, Any, ConditionResult]:
    """Returns satisfied if now is within the time window, unsatisfied if after hasn't arrived, impossible if before has passed."""  # noqa: E501
    now = datetime.now(tz=UTC)

    if before is not None and now >= before:
        reason = f"too late: now={now.isoformat()}, before={before.isoformat()}"
        return (DependencyState.IMPOSSIBLE, {"reason": reason})

    if after is not None and now < after:
        return (DependencyState.UNSATISFIED, {"after": after.isoformat()})

    yield from ()
    return (DependencyState.SATISFIED, {})


def time_dependency(
    before: datetime | None = None,
    after: datetime | None = None,
) -> Generator[Any, Any, DependencyResult]:
    return dependency(partial(time_condition, before, after), label="time")
