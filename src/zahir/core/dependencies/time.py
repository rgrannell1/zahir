# Dependency that waits until the current time is within a given window.
from collections.abc import Generator
from datetime import UTC, datetime
from functools import partial
from typing import Any

from zahir.core.dependencies.dependency import check, dependency
from zahir.core.exceptions import ImpossibleError
from zahir.core.zahir_types import DependencyResult


def _time_condition(
    before: datetime | None,
    after: datetime | None,
) -> Generator[Any, Any, Any]:
    """Returns True if now is within the time window, False if after hasn't arrived, raises ImpossibleError if before has passed."""  # noqa: E501
    now = datetime.now(tz=UTC)

    if before is not None and now >= before:
        raise ImpossibleError(f"too late: now={now.isoformat()}, before={before.isoformat()}")

    return not (after is not None and now < after)
    yield


def check_time_dependency(
    before: datetime | None = None,
    after: datetime | None = None,
) -> Generator[Any, Any, DependencyResult]:
    """Evaluate the time condition once; return satisfied or impossible without sleeping."""
    return check(partial(_time_condition, before, after), label="time")


def time_dependency(
    before: datetime | None = None,
    after: datetime | None = None,
) -> Generator[Any, Any, DependencyResult]:
    return dependency(partial(_time_condition, before, after), label="time")
