from collections import deque

from orbis import complete

from zahir.core.constants import OverseerMessage as OM
from zahir.core.evaluate.overseer import _handle_call, _handle_cast, _init
from zahir.core.zahir_types import OverseerState


def _make_state() -> OverseerState:
    return complete(_init("start", (1, 2)))


# init


def test_init_creates_initial_job_in_queue():
    """Proves init enqueues the starting job."""

    state = _make_state()
    assert len(state.queue) == 1
    assert state.queue[0].fn_name == "start"
    assert state.queue[0].args == (1, 2)


def test_init_sets_pending_to_one():
    """Proves init sets pending to 1 for the initial job."""

    assert _make_state().pending == 1


def test_init_starts_with_empty_concurrency():
    """Proves init starts with no concurrency slots registered."""

    assert _make_state().concurrency == {}


def test_init_starts_with_empty_semaphores():
    """Proves init starts with no semaphores registered."""

    assert _make_state().semaphores == {}


# handle_call dispatch


def test_handle_call_get_job_returns_job():
    """Proves handle_call dispatches get_job and returns the queued job."""

    state = _make_state()
    _, job = complete(_handle_call(state, (OM.GET_JOB, b"worker-pid")))
    assert job[1] == "start"


def test_handle_call_acquire_grants_slot():
    """Proves handle_call dispatches acquire and grants an available slot."""

    state = _make_state()
    _, result = complete(_handle_call(state, (OM.ACQUIRE, "workers", 4)))
    assert result is True


def test_handle_call_signal_returns_none_for_unknown_semaphore():
    """Proves handle_call dispatches signal and returns None for an unregistered semaphore."""

    _, result = complete(_handle_call(_make_state(), (OM.SIGNAL, "db")))
    assert result is None


def test_handle_call_is_done_false_initially():
    """Proves handle_call dispatches is_done and returns False when jobs are pending."""

    _, result = complete(_handle_call(_make_state(), OM.IS_DONE))
    assert result is False


# handle_cast dispatch


def test_handle_cast_enqueue_adds_job():
    """Proves handle_cast dispatches enqueue and adds a job to the queue."""

    state = complete(_handle_cast(_make_state(), (OM.ENQUEUE, "child", (42,), None, None)))
    assert any(job.fn_name == "child" for job in state.queue)


def test_handle_cast_job_done_decrements_pending():
    """Proves handle_cast dispatches job_done and decrements pending."""

    state = complete(_handle_cast(_make_state(), (OM.JOB_DONE, None, None, "result")))
    assert state.pending == 0


def test_handle_cast_release_decrements_slot():
    """Proves handle_cast dispatches release and decrements the concurrency count."""

    state = _make_state()
    state.concurrency["workers"] = (4, 2)
    state = complete(_handle_cast(state, (OM.RELEASE, "workers")))
    assert state.concurrency["workers"] == (4, 1)


def test_handle_cast_set_semaphore_stores_state():
    """Proves handle_cast dispatches set_semaphore and records the new state."""

    state = complete(_handle_cast(_make_state(), (OM.SET_SEMAPHORE, "db", "impossible")))
    assert state.semaphores["db"] == "impossible"


# round-trip


def test_enqueue_then_get_job_round_trips():
    """Proves a job enqueued via handle_cast is retrievable via handle_call."""

    state = _make_state()
    state = complete(_handle_cast(state, (OM.ENQUEUE, "child", (1,), None, 1000)))
    state, _ = complete(_handle_call(state, (OM.GET_JOB, b"worker-pid")))  # dequeue initial job
    _, job = complete(_handle_call(state, (OM.GET_JOB, b"worker-pid")))
    assert job[1] == "child"
    assert job[4] == 1000


def test_is_done_true_after_all_jobs_complete():
    """Proves is_done returns True once all jobs are dequeued and marked done."""

    state = _make_state()
    state, _ = complete(_handle_call(state, (OM.GET_JOB, b"worker-pid")))
    state = complete(_handle_cast(state, (OM.JOB_DONE, None, None, "result")))
    _, result = complete(_handle_call(state, OM.IS_DONE))
    assert result is True
