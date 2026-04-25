"""Aggregators for zahir-specific metrics derived from the telemetry event stream."""

from functools import partial

from bookman.aggregators import count_distinct, filter_events, group_by
from bookman.events import Event


def has_fn(ev: Event) -> bool:
    """True for events emitted by an active worker executing a job.

    Idle workers emit EGetJob events with no fn dimension; active workers emit
    EEnqueue, EJobComplete, and EJobFail which all carry fn_name.
    """

    return bool(ev.dim("fn"))


def get_pid(ev: Event) -> str:
    """Extract the worker pid dimension from an event."""

    return ev.dim("pid")


def active_pids_agg():
    """Aggregator counting unique worker pids on job-bearing events."""

    return filter_events(has_fn, count_distinct(get_pid))


def bucket_key(bucket_s: float, ev: Event) -> float:
    """Map an event to the start of its time bucket."""

    return (ev.at // bucket_s) * bucket_s


def active_cores_timeline(events: list, bucket_s: float = 1.0) -> dict[float, int]:
    """Unique active worker pids per time bucket across the event stream.

    Returns a dict mapping bucket-start timestamps to the count of unique pids
    that emitted at least one job-bearing event within that bucket.
    """

    return group_by(partial(bucket_key, bucket_s), active_pids_agg(), events)


def peak_active_cores(events: list, bucket_s: float = 1.0) -> int:
    """Maximum number of simultaneously active workers across all time buckets."""

    timeline = active_cores_timeline(events, bucket_s)
    return max(timeline.values(), default=0)
