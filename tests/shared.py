from collections.abc import Iterable
from datetime import UTC, datetime, timedelta
from typing import Any

from bookman.events import Event

NOW = datetime(2025, 1, 1, 12, 0, 0, tzinfo=UTC)
PAST = NOW - timedelta(hours=1)
FUTURE = NOW + timedelta(hours=1)


def user_events(events_iter: Iterable[Any]) -> list[Any]:
    """Filter an evaluate() stream to only user-emitted values.

    Tertius now emits infrastructure Event objects (spawn, process lifecycle, etc.)
    alongside user-emitted values. This strips the infrastructure layer so tests
    can assert on application-level output without breaking as event counts grow.
    """
    return [e for e in events_iter if not isinstance(e, Event)]
