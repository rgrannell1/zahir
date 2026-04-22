from bookman.create import point, span
from zahir.emit import PHASE_END, PHASE_START
from zahir.progress_bar.progress_bar_state import (
    JobStats,
    ProgressBarState,
    _ENQUEUE_TAG,
    _JOB_COMPLETE_TAG,
    _JOB_FAIL_TAG,
)


def _start(fn_name):
    return point({"tag": [_ENQUEUE_TAG], "fn": [fn_name], "id": ["s"], "phase": [PHASE_START]}, at=0.0)


def _end(fn_name, error=None):
    tag = _JOB_FAIL_TAG if error else _JOB_COMPLETE_TAG
    dims = {"tag": [tag], "fn": [fn_name], "id": ["s"], "phase": [PHASE_END]}
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
    state.update(point({"tag": [_ENQUEUE_TAG], "id": ["s"]}, at=0.0))
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
