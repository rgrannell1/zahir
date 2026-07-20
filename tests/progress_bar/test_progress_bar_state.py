from bookman.events import point, span

from zahir.core.commons.constants import JobTag, Phase
from zahir.progress_bar.progress_bar_state_model import ProgressBarState


def _start(fn_name, job_id="j1"):
    event_data = {
        "tag": [JobTag.ENQUEUE],
        "fn": [fn_name],
        "id": ["s"],
        "job_id": [job_id],
        "phase": [Phase.START],
    }
    return point(event_data, at=0.0)


def _execute(fn_name, job_id="j1"):
    event_data = {
        "tag": [JobTag.EXECUTE],
        "fn": [fn_name],
        "id": ["e"],
        "job_id": [job_id],
        "phase": [Phase.START],
    }
    return point(event_data, at=0.0)


def _end(fn_name, error=None, job_id="j1"):
    tag = JobTag.JOB_FAIL if error else JobTag.JOB_COMPLETE
    dims = {"tag": [tag], "fn": [fn_name], "id": ["s"], "job_id": [job_id], "phase": [Phase.END]}
    return span(dims, at=0.0, until=1.0)


def test_start_event_increments_started():
    """Proves an execute event (worker pickup) increments started, not an enqueue event."""
    state = ProgressBarState()
    state.update(_execute("job_a"))
    assert state.jobs["job_a"].started == 1


def test_enqueue_event_increments_total_not_started():
    """Proves an enqueue event increments total but leaves started at zero until pickup."""
    state = ProgressBarState()
    state.update(_start("job_a"))
    assert state.jobs["job_a"].total == 1
    assert state.jobs["job_a"].started == 0


def test_successful_end_increments_completed():
    state = ProgressBarState()
    state.update(_end("job_a"))
    assert state.jobs["job_a"].completed == 1


def test_failed_end_increments_failed():
    state = ProgressBarState()
    state.update(_end("job_a", error="timeout"))
    assert state.jobs["job_a"].failed == 1


def test_failed_end_does_not_increment_completed():
    state = ProgressBarState()
    state.update(_end("job_a", error="boom"))
    assert state.jobs["job_a"].completed == 0


def test_multiple_events_accumulate():
    state = ProgressBarState()
    state.update(_start("job_a", job_id="j1"))
    state.update(_execute("job_a", job_id="j1"))
    state.update(_start("job_a", job_id="j2"))
    state.update(_execute("job_a", job_id="j2"))
    state.update(_end("job_a", job_id="j1"))
    assert state.jobs["job_a"].started == 2
    assert state.jobs["job_a"].completed == 1


def test_events_for_different_fn_names_are_isolated():
    state = ProgressBarState()
    state.update(_start("job_a", job_id="j1"))
    state.update(_execute("job_a", job_id="j1"))
    state.update(_start("job_b", job_id="j2"))
    state.update(_execute("job_b", job_id="j2"))
    state.update(_end("job_b", job_id="j2"))
    assert state.jobs["job_a"].started == 1
    assert state.jobs["job_a"].completed == 0
    assert state.jobs["job_b"].started == 1
    assert state.jobs["job_b"].completed == 1


def test_event_without_fn_name_is_ignored():
    state = ProgressBarState()
    state.update(point({"tag": [JobTag.ENQUEUE], "id": ["s"]}, at=0.0))
    assert state.jobs == {}


def test_start_increments_total():
    state = ProgressBarState()
    state.update(_start("job_a", job_id="j1"))
    state.update(_start("job_a", job_id="j2"))
    assert state.jobs["job_a"].total == 2


def test_processed_is_completed_plus_failed():
    state = ProgressBarState()
    state.update(_end("job_a", job_id="j1"))
    state.update(_end("job_a", error="boom", job_id="j2"))
    assert state.jobs["job_a"].processed == 2


# intra-job progress


def _execute_on_pid(fn_name, pid):
    event_data = {
        "tag": [JobTag.EXECUTE],
        "fn": [fn_name],
        "id": ["e"],
        "pid": [str(pid)],
        "phase": [Phase.START],
    }
    return point(event_data, at=0.0)


def _progress_on_pid(pid, completed, total):
    event_data = {
        "tag": [JobTag.JOB_PROGRESS],
        "id": ["p"],
        "pid": [str(pid)],
        "completed": [str(completed)],
        "total": [str(total)],
    }
    return point(event_data, at=0.0)


def test_zero_total_progress_does_not_crash_the_display():
    """Proves a progress report over an empty batch (total=0) is treated as no known total."""
    state = ProgressBarState()
    state.update(_execute_on_pid("job_a", pid=7))
    state.update(_progress_on_pid(pid=7, completed=0, total=0))

    assert state.job_progress("job_a") == (0.0, None)


def test_known_total_progress_reports_mean_fraction():
    """Proves a progress report with a real total yields (fraction, total)."""
    state = ProgressBarState()
    state.update(_execute_on_pid("job_a", pid=7))
    state.update(_progress_on_pid(pid=7, completed=2, total=4))

    assert state.job_progress("job_a") == (0.5, 4)
