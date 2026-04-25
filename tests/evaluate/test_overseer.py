from functools import partial

from orbis import complete

from zahir.core.backends.memory import MemoryBackend
from zahir.core.constants import OverseerMessage as OM
from zahir.core.evaluate.overseer import _handle_call, _handle_cast, _init
from zahir.core.evaluate.overseer_handlers import CALL_HANDLERS, CAST_HANDLERS

# bind the unwrapped handler tables for unit tests
_call = partial(_handle_call, CALL_HANDLERS)
_cast = partial(_handle_cast, CAST_HANDLERS)


def _make_backend() -> MemoryBackend:
    backend = MemoryBackend()
    complete(_init(backend, "start", (1, 2)))
    return backend


# init


def test_init_creates_initial_job_in_queue():
    """Proves init enqueues the starting job."""

    backend = _make_backend()
    assert len(backend.queue) == 1
    assert backend.queue[0].fn_name == "start"
    assert backend.queue[0].args == (1, 2)


def test_init_sets_pending_to_one():
    """Proves init sets pending to 1 for the initial job."""

    assert _make_backend().pending == 1


def test_init_starts_with_empty_concurrency():
    """Proves init starts with no concurrency slots registered."""

    assert _make_backend().concurrency == {}


def test_init_starts_with_empty_semaphores():
    """Proves init starts with no semaphores registered."""

    assert _make_backend().semaphores == {}


# handle_call dispatch


def test_handle_call_get_job_returns_job():
    """Proves handle_call dispatches get_job and returns the queued job."""

    backend = _make_backend()
    _, job = complete(_call(backend, (OM.GET_JOB, b"worker-pid")))
    assert job[1] == "start"


def test_handle_call_acquire_grants_slot():
    """Proves handle_call dispatches acquire and grants an available slot."""

    backend = _make_backend()
    _, result = complete(_call(backend, (OM.ACQUIRE, "workers", 4)))
    assert result is True


def test_handle_call_signal_returns_none_for_unknown_semaphore():
    """Proves handle_call dispatches signal and returns None for an unregistered semaphore."""

    _, result = complete(_call(_make_backend(), (OM.SIGNAL, "db")))
    assert result is None


def test_handle_call_is_done_false_initially():
    """Proves handle_call dispatches is_done and returns False when jobs are pending."""

    _, result = complete(_call(_make_backend(), OM.IS_DONE))
    assert result is False


# handle_cast dispatch


def test_handle_cast_enqueue_adds_job():
    """Proves handle_cast dispatches enqueue and adds a job to the queue."""

    backend = complete(_cast(_make_backend(), (OM.ENQUEUE, "child", (42,), None, None)))
    assert any(job.fn_name == "child" for job in backend.queue)


def test_handle_cast_job_done_decrements_pending():
    """Proves handle_cast dispatches job_done and decrements pending."""

    backend = complete(_cast(_make_backend(), (OM.JOB_DONE, None, None, "result")))
    assert backend.pending == 0


def test_handle_cast_release_decrements_slot():
    """Proves handle_cast dispatches release and decrements the concurrency count."""

    backend = _make_backend()
    backend.concurrency["workers"] = (4, 2)
    backend = complete(_cast(backend, (OM.RELEASE, "workers")))
    assert backend.concurrency["workers"] == (4, 1)


def test_handle_cast_set_semaphore_stores_state():
    """Proves handle_cast dispatches set_semaphore and records the new state."""

    backend = complete(_cast(_make_backend(), (OM.SET_SEMAPHORE, "db", "impossible")))
    assert backend.semaphores["db"] == "impossible"


# round-trip


def test_enqueue_then_get_job_round_trips():
    """Proves a job enqueued via handle_cast is retrievable via handle_call."""

    backend = _make_backend()
    backend = complete(_cast(backend, (OM.ENQUEUE, "child", (1,), None, 1000)))
    backend, _ = complete(_call(backend, (OM.GET_JOB, b"worker-pid")))
    _, job = complete(_call(backend, (OM.GET_JOB, b"worker-pid")))
    assert job[1] == "child"
    assert job[4] == 1000


def test_is_done_true_after_all_jobs_complete():
    """Proves is_done returns True once all jobs are dequeued and marked done."""

    backend = _make_backend()
    backend, _ = complete(_call(backend, (OM.GET_JOB, b"worker-pid")))
    backend = complete(_cast(backend, (OM.JOB_DONE, None, None, "result")))
    _, result = complete(_call(backend, OM.IS_DONE))
    assert result is True
