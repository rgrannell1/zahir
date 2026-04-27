"""Aggregators for zahir-specific metrics derived from the telemetry event stream."""

from collections.abc import Generator, Iterable
from functools import partial

from bookman.aggregators import Aggregator, count_distinct, filter_events, group_by, map_extract, mean, stream_group_by, zip_all
from bookman.events import Event

from zahir.core.constants import JobTag, Phase


def has_fn(ev: Event) -> bool:
    """True for events emitted by an active worker executing a job.

    Idle workers emit EGetJob events with no fn dimension; active workers emit
    EEnqueue, EJobComplete, and EJobFail which all carry fn_name.
    """

    return bool(ev.dim("fn"))


def get_pid(ev: Event) -> str:
    """Extract the worker pid dimension from an event."""

    return ev.dim("pid")


def get_fn(ev: Event) -> str:
    """Extract the fn_name dimension from an event."""

    return ev.dim("fn")


def get_job_id(ev: Event) -> str:
    """Extract the job_id dimension from an event."""

    return ev.dim("job_id")


def is_enqueue_start(ev: Event) -> bool:
    """True when a job has been enqueued and is about to start."""

    return ev.dim("tag") == JobTag.ENQUEUE and ev.dim("phase") == Phase.START


def is_job_complete(ev: Event) -> bool:
    """True when a job has completed successfully."""

    return ev.dim("tag") == JobTag.JOB_COMPLETE and ev.dim("phase") == Phase.END


def is_job_fail(ev: Event) -> bool:
    """True when a job has failed."""

    return ev.dim("tag") == JobTag.JOB_FAIL and ev.dim("phase") == Phase.END


def job_stats_agg() -> Aggregator:
    """Aggregator producing [total, completed, failed] counts for a single fn_name.

    Use with stream_group_by(get_fn, job_stats_agg(), events) to get per-fn counts.
    total counts enqueue:start events; completed and failed count their respective end events.
    """

    return zip_all([
        filter_events(is_enqueue_start, count_distinct(get_job_id)),
        filter_events(is_job_complete, count_distinct(get_job_id)),
        filter_events(is_job_fail, count_distinct(get_job_id)),
    ])


def is_job_lifecycle(ev: Event) -> bool:
    """True for job_lifecycle span events, which carry the full enqueue-to-completion duration."""

    return ev.dim("tag") == JobTag.JOB_LIFECYCLE


def get_duration_ms(ev: Event) -> float:
    """Extract event duration in milliseconds."""

    return ev.duration("ms")


def none_if_zero(value: float) -> float | None:
    """Map 0.0 to None; bookman mean() uses 0.0 as its empty sentinel."""

    return value if value > 0.0 else None


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
