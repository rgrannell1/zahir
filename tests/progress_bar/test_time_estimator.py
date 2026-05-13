from bookman.events import point, span

from zahir.core.constants import JobTag, Phase
from zahir.progress_bar.progress_bar_service import (
    ProgressBarService,
    eta_remaining_ms,
    format_ms,
)


def _lifecycle(fn_name, duration_ms):
    """JOB_LIFECYCLE span carrying the measured wall-clock duration for one completed job."""
    return span({"tag": [JobTag.JOB_LIFECYCLE], "fn": [fn_name], "id": ["lc"]}, at=0.0, until=duration_ms / 1000.0)


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


def test_eta_remaining_ms_returns_none_when_nothing_completed():
    """Proves no estimate is produced before any jobs have finished."""
    assert eta_remaining_ms(0, 10, 5_000.0) is None


def test_eta_remaining_ms_returns_none_when_elapsed_is_zero():
    """Proves no estimate is produced when elapsed time is zero."""
    assert eta_remaining_ms(5, 10, 0.0) is None


def test_eta_remaining_ms_extrapolates_from_rate():
    """Proves ETA equals elapsed when completed equals remaining (rate projects 1:1)."""
    # 10 done in 1000ms → rate 0.01 jobs/ms; 10 remaining → 1000ms
    result = eta_remaining_ms(10, 10, 1_000.0)
    assert result == 1_000.0


def test_eta_remaining_ms_scales_with_remaining():
    """Proves ETA scales linearly with the number of remaining jobs."""
    # 5 done in 1000ms → rate 0.005 jobs/ms; 20 remaining → 4000ms
    result = eta_remaining_ms(5, 20, 1_000.0)
    assert result == 4_000.0


def test_eta_remaining_ms_returns_none_when_no_remaining():
    """Proves ETA is zero (still a valid float, not None) when nothing remains."""
    result = eta_remaining_ms(10, 0, 1_000.0)
    assert result == 0.0


def test_format_eta_returns_placeholder_when_nothing_completed():
    "Proves format_eta returns --:--:-- before any jobs finish."
    svc = _service()
    assert svc.format_eta(0, 10) == "--:--:--"


def test_format_ms_hh_mm_ss():
    assert format_ms(3_661_000.0) == "01:01:01"
