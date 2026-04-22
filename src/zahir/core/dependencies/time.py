# Dependency that waits until the current time is within a given window.
from collections.abc import Generator
from datetime import UTC, datetime
from functools import partial
from typing import Any

from tertius import ESleep

from zahir.core.dependencies.dependency import dependency
from zahir.core.zahir_types import DependencyResult
from zahir.core.exceptions import ImpossibleError


def _time_condition(
    before: datetime | None,
    after: datetime | None,
) -> Generator[Any, Any, Any]:
    """Returns True if now is within the time window, raises ImpossibleError if the window has passed."""
    now = datetime.now(tz=UTC)

    if before is not None and now >= before:
        raise ImpossibleError(
            f"too late: now={now.isoformat()}, before={before.isoformat()}"
        )

    if after is not None and now < after:
        ms = int((after - now).total_seconds() * 1000)
        yield ESleep(ms=ms)

    return True
    yield


def time_dependency(
    before: datetime | None = None,
    after: datetime | None = None,
) -> Generator[Any, Any, DependencyResult]:
    return dependency(partial(_time_condition, before, after))
