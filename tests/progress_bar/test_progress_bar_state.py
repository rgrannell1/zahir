from bookman.create import point, span
from zahir.core.constants import JobTag, Phase
from zahir.progress_bar.progress_bar_state_model import JobStats, ProgressBarState


def _start(fn_name):
    return point({"tag": [JobTag.ENQUEUE], "fn": [fn_name], "id": ["s"], "phase": [Phase.START]}, at=0.0)


def _end(fn_name, error=None):
    tag = JobTag.JOB_FAIL if error else JobTag.JOB_COMPLETE
    dims = {"tag": [tag], "fn": [fn_name], "id": ["s"], "phase": [Phase.END]}
    return span(dims, at=0.0, until=1.0)


def test_start_event_increments_started():
    state = ProgressBarState()
    state.update(_start("job_a"))
    assert state.jobs["job_a"].started == 1


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
    state.update(_start("job_a"))
    state.update(_start("job_a"))
    state.update(_end("job_a"))
    assert state.jobs["job_a"].started == 2
    assert state.jobs["job_a"].completed == 1


def test_events_for_different_fn_names_are_isolated():
    state = ProgressBarState()
    state.update(_start("job_a"))
    state.update(_start("job_b"))
    state.update(_end("job_b"))
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
    state.update(_start("job_a"))
    state.update(_start("job_a"))
    assert state.jobs["job_a"].total == 2


def test_processed_is_completed_plus_failed():
    state = ProgressBarState()
    state.update(_end("job_a"))
    state.update(_end("job_a", error="boom"))
    assert state.jobs["job_a"].processed == 2
