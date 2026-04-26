"""Tests proving the progress bar tracks and displays which dependency is blocking each job."""

import time

from bookman.events import point

from zahir.core.constants import DependencyTag, JobTag, Phase
from zahir.progress_bar.dep_labels import short_label
from zahir.progress_bar.descriptions_view import job_description
from zahir.progress_bar.progress_bar_state_model import JobStats, ProgressBarState


# --- event factories ---


def _enqueue(pid: int, fn_name: str):
    """ENQUEUE+START event that registers a worker pid onto a job."""
    return point(
        {"tag": [JobTag.ENQUEUE], "phase": [Phase.START], "pid": [str(pid)], "fn": [fn_name], "id": ["s"]},
        at=time.time(),
    )


def _waiting(pid: int, dep: str):
    """dep:waiting event emitted by the dependency polling loop."""
    return point({"tag": [DependencyTag.WAITING], "pid": [str(pid)], "dep": [dep]}, at=time.time())


def _satisfied(pid: int, dep: str):
    """dep:satisfied event emitted when a dependency is finally met or abandoned."""
    return point({"tag": [DependencyTag.SATISFIED], "pid": [str(pid)], "dep": [dep]}, at=time.time())


def _complete(pid: int, fn_name: str):
    """JOB_COMPLETE+END event — worker has finished a job."""
    return point(
        {"tag": [JobTag.JOB_COMPLETE], "phase": [Phase.END], "pid": [str(pid)], "fn": [fn_name], "id": ["s"]},
        at=time.time(),
    )


# --- waiting_deps state tracking ---


def test_waiting_dep_recorded_after_dep_waiting_event():
    """Proves waiting_deps returns the blocking dep after a dep:waiting event on a known pid."""
    state = ProgressBarState()
    state.update(_enqueue(100, "fetch_job"))
    state.update(_waiting(100, "memory resource"))
    result = state.waiting_deps("fetch_job")
    assert result == {"memory resource": 1}


def test_waiting_dep_cleared_after_dep_satisfied_event():
    """Proves waiting_deps returns empty once dep:satisfied arrives for the pid."""
    state = ProgressBarState()
    state.update(_enqueue(100, "fetch_job"))
    state.update(_waiting(100, "memory resource"))
    state.update(_satisfied(100, "memory resource"))
    assert state.waiting_deps("fetch_job") == {}


def test_waiting_dep_cleared_after_job_complete():
    """Proves waiting_deps returns empty when the job completes, even without a satisfied event."""
    state = ProgressBarState()
    state.update(_enqueue(100, "fetch_job"))
    state.update(_waiting(100, "cpu resource"))
    state.update(_complete(100, "fetch_job"))
    assert state.waiting_deps("fetch_job") == {}


def test_waiting_dep_not_attributed_to_wrong_fn():
    """Proves a dep:waiting event does not appear under a different fn_name's waiting_deps."""
    state = ProgressBarState()
    state.update(_enqueue(100, "fetch_job"))
    state.update(_waiting(100, "cpu resource"))
    assert state.waiting_deps("other_job") == {}


def test_multiple_pids_waiting_on_same_dep_aggregated():
    """Proves waiting_deps sums counts when several pids are blocked on the same dependency label."""
    state = ProgressBarState()
    state.update(_enqueue(100, "fetch_job"))
    state.update(_enqueue(200, "fetch_job"))
    state.update(_waiting(100, "memory resource"))
    state.update(_waiting(200, "memory resource"))
    assert state.waiting_deps("fetch_job") == {"memory resource": 2}


def test_multiple_pids_waiting_on_different_deps_both_reported():
    """Proves waiting_deps reports each distinct dep label separately."""
    state = ProgressBarState()
    state.update(_enqueue(100, "fetch_job"))
    state.update(_enqueue(200, "fetch_job"))
    state.update(_waiting(100, "memory resource"))
    state.update(_waiting(200, "cpu resource"))
    result = state.waiting_deps("fetch_job")
    assert result == {"memory resource": 1, "cpu resource": 1}


def test_waiting_dep_without_prior_enqueue_not_attributed():
    """Proves a dep:waiting event from an unknown pid does not appear under any fn's waiting_deps."""
    state = ProgressBarState()
    state.update(_waiting(999, "memory resource"))
    assert state.waiting_deps("fetch_job") == {}


# --- short_label mapping ---


def test_short_label_memory():
    """Proves memory resource deps resolve to MEM."""
    assert short_label("memory resource") == "MEM"


def test_short_label_cpu():
    """Proves cpu resource deps resolve to CPU."""
    assert short_label("cpu resource") == "CPU"


def test_short_label_concurrency_slot():
    """Proves concurrency slot deps resolve to LCK."""
    assert short_label("concurrency slot") == "LCK"


def test_short_label_concurrency_colon():
    """Proves concurrency: prefixed labels resolve to LCK."""
    assert short_label("concurrency:my_pool") == "LCK"


def test_short_label_semaphore_colon():
    """Proves semaphore: prefixed labels resolve to SEM."""
    assert short_label("semaphore:my_sem") == "SEM"


def test_short_label_sqlite():
    """Proves sqlite '...' labels resolve to SQL."""
    assert short_label("sqlite './data.db'") == "SQL"


def test_short_label_unknown_passthrough():
    """Proves an unrecognised dep string is returned unchanged."""
    assert short_label("some_custom_lock") == "some_custom_lock"


# --- job_description rendering ---


def _in_flight_stats() -> JobStats:
    """Two started, zero processed — gives in_flight=2."""
    stats = JobStats(total=2, started=2, completed=0, failed=0)
    return stats


def test_job_description_shows_waiting_when_blocked():
    """Proves the description contains the waiting indicator when jobs are blocked on a dep."""
    stats = _in_flight_stats()
    desc = job_description("fetch_job", stats, waiting={"memory resource": 1})
    assert "w:" in desc
    assert "MEM" in desc


def test_job_description_no_waiting_when_dict_empty():
    """Proves the description omits the waiting indicator when the waiting dict is empty."""
    stats = _in_flight_stats()
    desc = job_description("fetch_job", stats, waiting={})
    assert "w:" not in desc


def test_job_description_no_waiting_when_none():
    """Proves the description omits the waiting indicator when waiting is None."""
    stats = _in_flight_stats()
    desc = job_description("fetch_job", stats, waiting=None)
    assert "w:" not in desc


def test_job_description_waiting_count_shown():
    """Proves the waiting count is included when more than one job is blocked."""
    stats = _in_flight_stats()
    desc = job_description("fetch_job", stats, waiting={"memory resource": 3})
    assert "3" in desc
    assert "MEM" in desc


def test_job_description_fn_name_always_present():
    """Proves the fn_name appears in the description regardless of waiting state."""
    stats = _in_flight_stats()
    desc = job_description("fetch_job", stats, waiting={"cpu resource": 1})
    assert "fetch_job" in desc
