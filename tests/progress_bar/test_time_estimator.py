from zahir.progress_bar.events import ZahirSpanEnd, ZahirTelemetryEvent
from zahir.progress_bar.time_estimator import TimeEstimator


def _start(fn_name):
    return ZahirTelemetryEvent(
        span_id="s",
        tag="t",
        event="start",
        timestamp=0.0,
        attributes={"fn_name": fn_name},
    )


def _end(fn_name, duration_ms, error=None):
    return ZahirSpanEnd(
        span_id="s",
        tag="t",
        event="end",
        timestamp=1.0,
        attributes={"fn_name": fn_name},
        duration_ms=duration_ms,
        error=error,
    )


# mean_duration_ms


def test_mean_duration_returns_none_with_no_data():
    est = TimeEstimator()
    assert est.mean_duration_ms("job_a") is None


def test_mean_duration_single_sample():
    est = TimeEstimator()
    est.update(_end("job_a", 200.0))
    assert est.mean_duration_ms("job_a") == 200.0


def test_mean_duration_multiple_samples():
    est = TimeEstimator()
    est.update(_end("job_a", 100.0))
    est.update(_end("job_a", 300.0))
    assert est.mean_duration_ms("job_a") == 200.0


def test_mean_duration_isolated_per_fn_name():
    est = TimeEstimator()
    est.update(_end("job_a", 100.0))
    est.update(_end("job_b", 500.0))
    assert est.mean_duration_ms("job_a") == 100.0
    assert est.mean_duration_ms("job_b") == 500.0


# format_eta


def test_format_eta_returns_placeholder_with_no_in_flight():
    est = TimeEstimator()
    assert est.format_eta() == "--:--:--"


def test_format_eta_returns_placeholder_when_no_duration_data():
    """In-flight jobs with no completed samples produce no estimate."""
    est = TimeEstimator()
    est.update(_start("job_a"))
    assert est.format_eta() == "--:--:--"


def test_format_eta_computes_from_in_flight_and_mean():
    est = TimeEstimator()
    est.update(_end("job_a", 60_000.0))   # mean = 60s
    est.update(_start("job_a"))           # 1 in flight
    assert est.format_eta() == "00:01:00"


def test_format_eta_multiplies_count_by_mean():
    est = TimeEstimator()
    est.update(_end("job_a", 10_000.0))
    est.update(_start("job_a"))
    est.update(_start("job_a"))
    est.update(_start("job_a"))           # 3 in flight × 10s = 30s
    assert est.format_eta() == "00:00:30"


def test_format_eta_sums_across_fn_names():
    est = TimeEstimator()
    est.update(_end("job_a", 30_000.0))
    est.update(_end("job_b", 30_000.0))
    est.update(_start("job_a"))           # 30s
    est.update(_start("job_b"))           # 30s → total 60s
    assert est.format_eta() == "00:01:00"


def test_format_eta_hours_minutes_seconds():
    est = TimeEstimator()
    est.update(_end("job_a", 3_661_000.0))  # 1h 1m 1s
    est.update(_start("job_a"))
    assert est.format_eta() == "01:01:01"


def test_in_flight_decrements_on_end():
    est = TimeEstimator()
    est.update(_end("job_a", 1_000.0))
    est.update(_start("job_a"))
    est.update(_end("job_a", 1_000.0))    # clears in-flight
    assert est.format_eta() == "--:--:--"


def test_in_flight_clamps_to_zero_on_out_of_order_end():
    """End events can arrive before start due to concurrency; count must not go negative."""
    est = TimeEstimator()
    est.update(_end("job_a", 1_000.0))    # end before start
    est.update(_end("job_a", 1_000.0))
    assert est.format_eta() == "--:--:--"


def test_event_without_fn_name_is_ignored():
    est = TimeEstimator()
    event = ZahirTelemetryEvent(span_id="s", tag="t", event="start", timestamp=0.0, attributes={})
    est.update(event)
    assert est.format_eta() == "--:--:--"
