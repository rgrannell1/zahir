from collections.abc import Generator
from datetime import UTC, datetime, timedelta
from typing import Any, Literal

import psutil

from tertius import ESleep

from zahir.core.constants import CPU_SAMPLE_INTERVAL_S, DEPENDENCY_DELAY_MS
from zahir.core.effects import EImpossible, ESatisfied

type ResourceType = Literal["cpu", "memory"]


def _get_usage(resource: ResourceType) -> float:
    match resource:
        case "cpu":
            return psutil.cpu_percent(interval=CPU_SAMPLE_INTERVAL_S)
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
) -> Generator[ESatisfied | EImpossible | ESleep, None, ESatisfied | EImpossible]:
    timeout_at = (
        datetime.now(tz=UTC) + timedelta(seconds=timeout)
        if timeout is not None
        else None
    )

    while True:
        now = datetime.now(tz=UTC)

        if timeout_at is not None and now >= timeout_at:
            event = EImpossible(reason=f"{resource} limit timed out after {timeout}s")
            yield event
            return event

        if _get_usage(resource) <= max_percent:
            event = ESatisfied(metadata=_metadata(resource, max_percent, timeout_at))
            yield event
            return event

        yield ESleep(ms=DEPENDENCY_DELAY_MS)
