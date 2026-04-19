from collections import deque

from constants import ACQUIRE, ENQUEUE, GET_JOB, IS_DONE, JOB_DONE, RELEASE, SET_SEMAPHORE, SIGNAL
from evaluate.overseer import Overseer
from zahir_types import OverseerState


overseer = Overseer()


def _init() -> OverseerState:
    return overseer.init("start", (1, 2))


# init

def test_init_creates_initial_job_in_queue():
    """Proves init enqueues the starting job."""

    state = _init()
    assert len(state.queue) == 1
    assert state.queue[0].fn_name == "start"
    assert state.queue[0].args == (1, 2)


def test_init_sets_pending_to_one():
    """Proves init sets pending to 1 for the initial job."""

    assert _init().pending == 1


def test_init_starts_with_empty_concurrency():
    """Proves init starts with no concurrency slots registered."""

    assert _init().concurrency == {}


def test_init_starts_with_empty_semaphores():
    """Proves init starts with no semaphores registered."""

    assert _init().semaphores == {}


# handle_call dispatch

def test_handle_call_get_job_returns_job():
    """Proves handle_call dispatches get_job and returns the queued job."""

    state = _init()
    _, job = overseer.handle_call(state, GET_JOB)
    assert job[0] == "start"


def test_handle_call_acquire_grants_slot():
    """Proves handle_call dispatches acquire and grants an available slot."""

    state = _init()
    _, result = overseer.handle_call(state, (ACQUIRE, "workers", 4))
    assert result is True


def test_handle_call_signal_raises_for_unknown_semaphore():
    """Proves handle_call dispatches signal and raises for unknown semaphores."""

    import pytest
    with pytest.raises(KeyError, match="has not been set"):
        overseer.handle_call(_init(), (SIGNAL, "db"))


def test_handle_call_is_done_false_initially():
    """Proves handle_call dispatches is_done and returns False when jobs are pending."""

    _, result = overseer.handle_call(_init(), IS_DONE)
    assert result is False


# handle_cast dispatch

def test_handle_cast_enqueue_adds_job():
    """Proves handle_cast dispatches enqueue and adds a job to the queue."""

    state = overseer.handle_cast(_init(), (ENQUEUE, "child", (42,), None, None))
    assert any(job.fn_name == "child" for job in state.queue)


def test_handle_cast_job_done_decrements_pending():
    """Proves handle_cast dispatches job_done and decrements pending."""

    state = overseer.handle_cast(_init(), JOB_DONE)
    assert state.pending == 0


def test_handle_cast_release_decrements_slot():
    """Proves handle_cast dispatches release and decrements the concurrency count."""

    state = _init()
    state.concurrency["workers"] = (4, 2)
    state = overseer.handle_cast(state, (RELEASE, "workers"))
    assert state.concurrency["workers"] == (4, 1)


def test_handle_cast_set_semaphore_stores_state():
    """Proves handle_cast dispatches set_semaphore and records the new state."""

    state = overseer.handle_cast(_init(), (SET_SEMAPHORE, "db", "impossible"))
    assert state.semaphores["db"] == "impossible"


# round-trip

def test_enqueue_then_get_job_round_trips():
    """Proves a job enqueued via handle_cast is retrievable via handle_call."""

    state = _init()
    state = overseer.handle_cast(state, (ENQUEUE, "child", (1,), None, 1000))
    overseer.handle_call(state, GET_JOB)  # dequeue initial job
    _, job = overseer.handle_call(state, GET_JOB)
    assert job[0] == "child"
    assert job[3] == 1000


def test_is_done_true_after_all_jobs_complete():
    """Proves is_done returns True once all jobs are dequeued and marked done."""

    state = _init()
    state, _ = overseer.handle_call(state, GET_JOB)
    state = overseer.handle_cast(state, JOB_DONE)
    _, result = overseer.handle_call(state, IS_DONE)
    assert result is True
