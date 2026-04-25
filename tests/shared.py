from collections.abc import Iterable
from datetime import UTC, datetime, timedelta
from typing import Any

from bookman.events import Event

_DEFAULT_WINDOW_MS = 50.0

type Interval = tuple[float, float]

NOW = datetime(2025, 1, 1, 12, 0, 0, tzinfo=UTC)
PAST = NOW - timedelta(hours=1)
FUTURE = NOW + timedelta(hours=1)


def mean_active_cores(events: list[Any], window_ms: float = _DEFAULT_WINDOW_MS) -> float:
    """Estimate mean concurrency by counting unique worker PIDs per time window.

    Divides the event stream's wall-clock span into windows of window_ms and counts
    unique PIDs that emitted at least one event in each window, then averages across
    all windows. Requires events produced with make_telemetry() so the pid dim is set.
    """
    pid_times = [(e.at, e.dim("pid")) for e in events if isinstance(e, Event) and e.dim("pid")]
    if not pid_times:
        return 0.0

    start = min(at for at, _ in pid_times)
    end = max(at for at, _ in pid_times)
    window_s = window_ms / 1000.0
    n_windows = max(1, int((end - start) / window_s))

    counts = []
    for idx in range(n_windows):
        window_start = start + idx * window_s
        window_end = window_start + window_s
        active = {pid for at, pid in pid_times if window_start <= at < window_end}
        counts.append(len(active))

    return sum(counts) / len(counts)


def peak_concurrent(intervals: list[Interval]) -> int:
    """Return the maximum number of intervals overlapping at any single point in time."""
    endpoints = []
    for start, end in intervals:
        endpoints.append((start, +1))
        endpoints.append((end, -1))
    endpoints.sort()
    peak = 0
    current = 0
    for _, change in endpoints:
        current += change
        peak = max(peak, current)
    return peak


def user_events(events_iter: Iterable[Any]) -> list[Any]:
    """Filter an evaluate() stream to only user-emitted values.

    Tertius now emits infrastructure Event objects (spawn, process lifecycle, etc.)
    alongside user-emitted values. This strips the infrastructure layer so tests
    can assert on application-level output without breaking as event counts grow.
    """
    return [e for e in events_iter if not isinstance(e, Event)]
