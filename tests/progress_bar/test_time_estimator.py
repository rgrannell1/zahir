from bookman.events import point, span

from zahir.core.constants import JobTag, Phase
from zahir.progress_bar.progress_bar_service import (
    ProgressBarService,
    eta_remaining_ms,
    format_ms,
)
from zahir.progress_bar.progress_bar_state_model import JobStats


def _start(fn_name, job_id="j1"):
    return point({"tag": [JobTag.ENQUEUE], "fn": [fn_name], "id": ["s"], "job_id": [job_id], "phase": [Phase.START]}, at=0.0)


def _lifecycle(fn_name, duration_ms):
    """JOB_LIFECYCLE span carrying the measured wall-clock duration for one completed job."""
    return span({"tag": [JobTag.JOB_LIFECYCLE], "fn": [fn_name], "id": ["lc"]}, at=0.0, until=duration_ms / 1000.0)


def _end(fn_name, job_id="j1", error=False):
    tag = JobTag.JOB_FAIL if error else JobTag.JOB_COMPLETE
    return span({"tag": [tag], "fn": [fn_name], "id": ["s"], "job_id": [job_id], "phase": [Phase.END]}, at=0.0, until=0.1)


def _service(*events):
    svc = ProgressBarService()
    for ev in events:
        svc.update(ev)
    return svc


def test_mean_duration_returns_none_with_no_data():
    svc = _service()
    assert svc.mean_duration_ms("job_a") is None


def test_mean_duration_single_sample():
    svc = _service(_lifecycle("job_a", 200.0))
    assert svc.mean_duration_ms("job_a") == 200.0


def test_mean_duration_multiple_samples():
    svc = _service(_lifecycle("job_a", 100.0), _lifecycle("job_a", 300.0))
    assert svc.mean_duration_ms("job_a") == 200.0


def test_mean_duration_isolated_per_fn_name():
    svc = _service(_lifecycle("job_a", 100.0), _lifecycle("job_b", 500.0))
    assert svc.mean_duration_ms("job_a") == 100.0
    assert svc.mean_duration_ms("job_b") == 500.0


def test_format_eta_returns_placeholder_with_no_in_flight():
    svc = _service()
    assert svc.format_eta() == "--:--:--"


def test_format_eta_returns_placeholder_when_no_duration_data():
    """Proves in-flight jobs with no lifecycle samples produce no estimate."""
    svc = _service(_start("job_a"))
    assert svc.format_eta() == "--:--:--"


def test_format_eta_computes_from_in_flight_and_mean():
    svc = _service(
        _lifecycle("job_a", 60_000.0),
        _start("job_a", job_id="j1"),  # 1 in-flight
    )
    assert svc.format_eta() == "00:01:00"


def test_format_eta_multiplies_count_by_mean():
    svc = _service(
        _lifecycle("job_a", 10_000.0),
        _start("job_a", job_id="j1"),
        _start("job_a", job_id="j2"),
        _start("job_a", job_id="j3"),  # 3 in-flight x 10s = 30s
    )
    assert svc.format_eta() == "00:00:30"


def test_format_eta_sums_across_fn_names():
    svc = _service(
        _lifecycle("job_a", 30_000.0),
        _lifecycle("job_b", 30_000.0),
        _start("job_a", job_id="j1"),  # 30s
        _start("job_b", job_id="j2"),  # 30s → total 60s
    )
    assert svc.format_eta() == "00:01:00"


def test_format_eta_hours_minutes_seconds():
    svc = _service(
        _lifecycle("job_a", 3_661_000.0),
        _start("job_a", job_id="j1"),
    )
    assert svc.format_eta() == "01:01:01"


def test_in_flight_clears_when_end_arrives():
    svc = _service(
        _lifecycle("job_a", 1_000.0),
        _start("job_a", job_id="j1"),
        _end("job_a", job_id="j1"),  # clears in-flight
    )
    assert svc.format_eta() == "--:--:--"


def test_in_flight_clamps_to_zero_on_out_of_order_end():
    """Proves end events arriving before start do not cause a negative in-flight count."""
    svc = _service(
        _lifecycle("job_a", 1_000.0),
        _end("job_a", job_id="j1"),
        _end("job_a", job_id="j2"),
    )
    assert svc.format_eta() == "--:--:--"


def test_format_eta_uses_partial_data_when_some_types_have_no_history():
    """Proves ETA renders using job types with history, skipping those without."""
    svc = _service(
        _lifecycle("job_a", 30_000.0),
        _start("job_a", job_id="j1"),  # 1 in-flight x 30s = 30s
        _start("job_b", job_id="j2"),  # no lifecycle data yet — skipped
    )
    assert svc.format_eta() == "00:00:30"


def test_event_without_fn_name_is_ignored():
    svc = _service(point({"tag": [JobTag.ENQUEUE], "id": ["s"]}, at=0.0))
    assert svc.format_eta() == "--:--:--"


def test_eta_remaining_ms_divides_by_mean_cores():
    """Proves ETA is halved when two cores are running in parallel."""
    jobs = {"job_a": JobStats(total=2, started=2, completed=1, failed=0, mean_ms=60_000.0)}
    single = eta_remaining_ms(jobs, 1.0)
    double = eta_remaining_ms(jobs, 2.0)
    assert single == 60_000.0
    assert double == 30_000.0


def test_eta_remaining_ms_clamps_cores_to_one():
    """Proves mean_cores=0 is treated as 1 to avoid division by zero."""
    jobs = {"job_a": JobStats(total=2, started=2, completed=1, failed=0, mean_ms=30_000.0)}
    assert eta_remaining_ms(jobs, 0.0) == eta_remaining_ms(jobs, 1.0)


def test_format_ms_hh_mm_ss():
    assert format_ms(3_661_000.0) == "01:01:01"
