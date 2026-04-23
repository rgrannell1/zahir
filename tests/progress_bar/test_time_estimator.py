from bookman.create import point, span
from zahir.emit import PHASE_END, PHASE_START
from zahir.progress_bar.progress_bar_state_model import ENQUEUE_TAG, JOB_COMPLETE_TAG, JOB_FAIL_TAG
from zahir.progress_bar.time_estimator_service import TimeEstimator


def _start(fn_name, job_id="j1"):
    return point({"tag": [ENQUEUE_TAG], "fn": [fn_name], "id": ["s"], "job_id": [job_id], "phase": [PHASE_START]}, at=0.0)


def _end(fn_name, duration_ms, error=None, job_id="j1"):
    tag = JOB_FAIL_TAG if error else JOB_COMPLETE_TAG
    return span(
        {"tag": [tag], "fn": [fn_name], "id": ["s"], "job_id": [job_id], "phase": [PHASE_END]}, at=0.0, until=duration_ms / 1000.0
    )


def test_mean_duration_returns_none_with_no_data():
    est = TimeEstimator()
    assert est.mean_duration_ms("job_a") is None


def test_mean_duration_single_sample():
    est = TimeEstimator()
    est.update(_start("job_a"))
    est.update(_end("job_a", 200.0))
    assert est.mean_duration_ms("job_a") == 200.0


def test_mean_duration_multiple_samples():
    est = TimeEstimator()
    est.update(_start("job_a", job_id="j1"))
    est.update(_end("job_a", 100.0, job_id="j1"))
    est.update(_start("job_a", job_id="j2"))
    est.update(_end("job_a", 300.0, job_id="j2"))
    assert est.mean_duration_ms("job_a") == 200.0


def test_mean_duration_isolated_per_fn_name():
    est = TimeEstimator()
    est.update(_start("job_a", job_id="j1"))
    est.update(_end("job_a", 100.0, job_id="j1"))
    est.update(_start("job_b", job_id="j2"))
    est.update(_end("job_b", 500.0, job_id="j2"))
    assert est.mean_duration_ms("job_a") == 100.0
    assert est.mean_duration_ms("job_b") == 500.0


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
    est.update(_start("job_a", job_id="j1"))
    est.update(_end("job_a", 60_000.0, job_id="j1"))  # mean = 60s
    est.update(_start("job_a", job_id="j2"))  # 1 in flight
    assert est.format_eta() == "00:01:00"


def test_format_eta_multiplies_count_by_mean():
    est = TimeEstimator()
    est.update(_start("job_a", job_id="j1"))
    est.update(_end("job_a", 10_000.0, job_id="j1"))
    est.update(_start("job_a", job_id="j2"))
    est.update(_start("job_a", job_id="j3"))
    est.update(_start("job_a", job_id="j4"))  # 3 in flight × 10s = 30s
    assert est.format_eta() == "00:00:30"


def test_format_eta_sums_across_fn_names():
    est = TimeEstimator()
    est.update(_start("job_a", job_id="j1"))
    est.update(_end("job_a", 30_000.0, job_id="j1"))
    est.update(_start("job_b", job_id="j2"))
    est.update(_end("job_b", 30_000.0, job_id="j2"))
    est.update(_start("job_a", job_id="j3"))  # 30s
    est.update(_start("job_b", job_id="j4"))  # 30s → total 60s
    assert est.format_eta() == "00:01:00"


def test_format_eta_hours_minutes_seconds():
    est = TimeEstimator()
    est.update(_start("job_a", job_id="j1"))
    est.update(_end("job_a", 3_661_000.0, job_id="j1"))  # 1h 1m 1s
    est.update(_start("job_a", job_id="j2"))
    assert est.format_eta() == "01:01:01"


def test_in_flight_decrements_on_end():
    est = TimeEstimator()
    est.update(_end("job_a", 1_000.0, job_id="j1"))  # out-of-order end, no matching start
    est.update(_start("job_a", job_id="j2"))
    est.update(_end("job_a", 1_000.0, job_id="j2"))  # clears in-flight
    assert est.format_eta() == "--:--:--"


def test_in_flight_clamps_to_zero_on_out_of_order_end():
    """End events can arrive before start due to concurrency; count must not go negative."""
    est = TimeEstimator()
    est.update(_end("job_a", 1_000.0, job_id="j1"))  # end before start
    est.update(_end("job_a", 1_000.0, job_id="j2"))
    assert est.format_eta() == "--:--:--"


def test_event_without_fn_name_is_ignored():
    est = TimeEstimator()
    est.update(point({"tag": [ENQUEUE_TAG], "id": ["s"]}, at=0.0))
    assert est.format_eta() == "--:--:--"
