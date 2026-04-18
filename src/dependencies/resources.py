from collections.abc import Generator
from datetime import UTC, datetime, timedelta
from typing import Any, Literal

import psutil

from tertius import ESleep

from constants import DEPENDENCY_DELAY_MS
from effects import EImpossible, ESatisfied

type ResourceType = Literal["cpu"] | Literal["memory"]


def _get_usage(resource: ResourceType) -> float:
    match resource:
        case "cpu":
            return psutil.cpu_percent(interval=0.1)
        case "memory":
            return psutil.virtual_memory().percent


def _metadata(
    resource: ResourceType,
    max_percent: float,
    timeout_at: datetime | None,
) -> dict[str, Any]:
    return {
        "resource": resource,
        "max_percent": max_percent,
        "timeout_at": timeout_at.isoformat() if timeout_at else None,
    }


def resource_dependency(
    resource: ResourceType,
    max_percent: float,
    timeout: float | None = None,
) -> Generator[ESatisfied | EImpossible | ESleep, None, None]:
    timeout_at = datetime.now(tz=UTC) + timedelta(seconds=timeout) if timeout is not None else None

    while True:
        now = datetime.now(tz=UTC)

        if timeout_at is not None and now >= timeout_at:
            yield EImpossible(reason=f"{resource} limit timed out after {timeout}s")
            return

        if _get_usage(resource) <= max_percent:
            yield ESatisfied(metadata=_metadata(resource, max_percent, timeout_at))
            return

        yield ESleep(ms=DEPENDENCY_DELAY_MS)
