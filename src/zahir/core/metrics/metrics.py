"""Aggregators for zahir-specific metrics derived from the telemetry event stream."""

from collections.abc import Generator, Iterable
from functools import partial

from bookman.aggregators import (
    Aggregator,
    count_distinct,
    filter_events,
    group_by,
    map_extract,
    mean,
    stream_group_by,
    zip_all,
)

from zahir.core.metrics.selectors import (
    get_duration_ms,
    get_job_id,
    get_pid,
    has_fn,
    is_enqueue_start,
    is_job_complete,
    is_job_fail,
    is_job_lifecycle,
)


def none_if_zero(value: float) -> float | None:
    """Map 0.0 to None; bookman mean() uses 0.0 as its empty sentinel."""

    return value if value > 0.0 else None


def job_stats_agg() -> Aggregator:
    """Aggregator producing [total, completed, failed] counts for a single fn_name.

    Use with stream_group_by(get_fn, job_stats_agg(), events) to get per-fn counts.
    total counts enqueue:start events; completed and failed count their respective end events.
    """

    return zip_all(
        [
            filter_events(is_enqueue_start, count_distinct(get_job_id)),
            filter_events(is_job_complete, count_distinct(get_job_id)),
            filter_events(is_job_fail, count_distinct(get_job_id)),
        ]
    )


def job_duration_mean_agg() -> Aggregator:
    """Mean job duration (ms) per fn, from the job_lifecycle span events emitted by telemetry.

    Returns None when no lifecycle events have been seen.
    """

    raw = filter_events(is_job_lifecycle, mean(get_duration_ms))
    return map_extract(none_if_zero, raw)


def per_fn_progress_agg() -> Aggregator:
    """Combined per-fn aggregator producing [[total, completed, failed], mean_ms].

    Use with stream_group_by(get_fn, per_fn_progress_agg(), events).
    """

    return zip_all([job_stats_agg(), job_duration_mean_agg()])


def active_pids_agg() -> Aggregator:
    """Aggregator counting unique worker pids on job-bearing events."""

    return filter_events(has_fn, count_distinct(get_pid))


def bucket_key(bucket_s: float, ev) -> float:
    """Map an event to the start of its time bucket."""

    return (ev.at // bucket_s) * bucket_s


def active_cores_timeline(events: Iterable, bucket_s: float = 1.0) -> Generator[dict[float, int], None, None]:
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
