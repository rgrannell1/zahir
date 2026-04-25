"""Aggregators for zahir-specific metrics derived from the telemetry event stream."""

from collections.abc import Generator, Iterable
from functools import partial

from bookman.aggregators import count_distinct, filter_events, group_by, stream_group_by
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


def active_cores_timeline(
    events: Iterable, bucket_s: float = 1.0
) -> Generator[dict[float, int], None, None]:
    """Yield an updated per-bucket active core count after each event in the stream.

    Each yielded dict maps bucket-start timestamps to the count of unique worker pids
    that emitted at least one job-bearing event within that bucket.
    Accepts any iterable — including a live evaluate() generator.
    """

    return stream_group_by(partial(bucket_key, bucket_s), active_pids_agg(), events)


def peak_active_cores(events: Iterable, bucket_s: float = 1.0) -> int:
    """Maximum number of simultaneously active workers across all time buckets.

    Consumes the full event stream. For a live stream, use active_cores_timeline instead.
    """

    timeline = group_by(partial(bucket_key, bucket_s), active_pids_agg(), events)
    return max(timeline.values(), default=0)
