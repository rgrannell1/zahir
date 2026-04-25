# Tests for MemoryBackend methods — proves each storage operation behaves correctly in isolation.
from collections import deque

from zahir.core.backends.memory import MemoryBackend
from zahir.core.zahir_types import JobSpec

WORKER = b"worker-pid"


def _backend(**kwargs) -> MemoryBackend:
    return MemoryBackend(**kwargs)


# get_job


def test_get_job_returns_none_when_queue_empty_and_no_results():
    """Proves get_job returns None when no jobs are queued and no results are buffered."""

    assert _backend().get_job(WORKER) is None


def test_get_job_returns_job_tuple_from_queue():
    """Proves get_job returns a ("job", ...) tuple from the queue when no results are pending."""

    spec = JobSpec(fn_name="process", args=(1,), reply_to=None, timeout_ms=5000, sequence_number=3)
    result = _backend(queue=deque([spec])).get_job(WORKER)
    assert result == ("job", "process", (1,), None, 5000, 3)


def test_get_job_removes_job_from_queue():
    """Proves get_job pops the job from the queue."""

    spec = JobSpec(fn_name="fn", args=(), reply_to=None)
    backend = _backend(queue=deque([spec]))
    backend.get_job(WORKER)
    assert len(backend.queue) == 0


def test_get_job_preserves_fifo_order():
    """Proves get_job dequeues jobs in FIFO order."""

    specs = [JobSpec(fn_name=f"fn{idx}", args=(), reply_to=None) for idx in range(3)]
    result = _backend(queue=deque(specs)).get_job(WORKER)
    assert result[1] == "fn0"


def test_get_job_returns_buffered_result_before_queue():
    """Proves get_job returns a pending result for this worker before taking a new job from the queue."""

    pending = {WORKER: deque([(42, "result")])}
    spec = JobSpec(fn_name="fn", args=(), reply_to=None)
    work = _backend(queue=deque([spec]), pending_results=pending).get_job(WORKER)
    assert work == ("result", 42, "result")


def test_get_job_result_is_worker_specific():
    """Proves get_job only returns results addressed to the requesting worker."""

    other_worker = b"other-pid"
    pending = {other_worker: deque([(1, "result")])}
    assert _backend(pending_results=pending).get_job(WORKER) is None


# enqueue


def test_enqueue_adds_job_to_queue():
    """Proves enqueue appends a new JobSpec to the queue."""

    backend = _backend()
    backend.enqueue("process", (1,), None, None, None)
    assert len(backend.queue) == 1
    assert backend.queue[0].fn_name == "process"


def test_enqueue_increments_pending():
    """Proves enqueue increments the pending job count."""

    backend = _backend(pending=2)
    backend.enqueue("fn", (), None, None, None)
    assert backend.pending == 3


def test_enqueue_stores_timeout_ms():
    """Proves enqueue passes timeout_ms through to the JobSpec."""

    backend = _backend()
    backend.enqueue("fn", (), None, 3000, None)
    assert backend.queue[0].timeout_ms == 3000


# job_done


def test_job_done_decrements_pending():
    """Proves job_done decrements the pending count."""

    backend = _backend(pending=3)
    backend.job_done(WORKER, 1, "result")
    assert backend.pending == 2


def test_job_done_buffers_result_for_worker():
    """Proves job_done stores (sequence_number, body) in pending_results for the given worker."""

    backend = _backend(pending=1)
    backend.job_done(WORKER, 7, "result")
    assert list(backend.pending_results[WORKER]) == [(7, "result")]


def test_job_done_with_none_reply_to_stores_root_result():
    """Proves job_done with reply_to=None stores the result as root_result."""

    backend = _backend(pending=1)
    backend.job_done(None, None, "final")
    assert backend.pending == 0
    assert backend.root_result == "final"


# job_failed


def test_job_failed_decrements_pending():
    """Proves job_failed decrements the pending count."""

    backend = _backend(pending=2)
    backend.job_failed(ValueError("boom"))
    assert backend.pending == 1


def test_job_failed_stores_root_error():
    """Proves job_failed records the error as root_error."""

    err = ValueError("boom")
    backend = _backend(pending=1)
    backend.job_failed(err)
    assert backend.root_error is err


def test_job_failed_does_not_overwrite_existing_root_error():
    """Proves job_failed keeps the first error and ignores subsequent ones."""

    first = ValueError("first")
    second = ValueError("second")
    backend = _backend(pending=2, root_error=first)
    backend.job_failed(second)
    assert backend.root_error is first


# acquire


def test_acquire_grants_slot_when_under_limit():
    """Proves acquire returns True and records the slot when under limit."""

    backend = _backend()
    result = backend.acquire("workers", 4)
    assert result is True
    assert backend.concurrency["workers"] == (4, 1)


def test_acquire_denies_slot_when_at_limit():
    """Proves acquire returns False when the concurrency limit is reached."""

    backend = _backend(concurrency={"workers": (2, 2)})
    assert backend.acquire("workers", 2) is False


def test_acquire_respects_existing_limit():
    """Proves acquire uses the stored limit rather than the requested one."""

    backend = _backend(concurrency={"workers": (2, 1)})
    result = backend.acquire("workers", 99)
    assert result is True
    assert backend.concurrency["workers"] == (2, 2)


# release


def test_release_decrements_active_count():
    """Proves release decrements the active count for a known slot."""

    backend = _backend(concurrency={"workers": (4, 3)})
    backend.release("workers")
    assert backend.concurrency["workers"] == (4, 2)


def test_release_does_not_go_below_zero():
    """Proves release clamps the active count at zero."""

    backend = _backend(concurrency={"workers": (4, 0)})
    backend.release("workers")
    assert backend.concurrency["workers"][1] == 0


def test_release_ignores_unknown_name():
    """Proves release is a no-op for an unregistered slot name."""

    backend = _backend()
    backend.release("unknown")
    assert "unknown" not in backend.concurrency


# signal


def test_signal_returns_stored_state():
    """Proves signal returns the current semaphore state."""

    assert _backend(semaphores={"db": "satisfied"}).signal("db") == "satisfied"


def test_signal_returns_none_for_unknown_name():
    """Proves signal returns None for a semaphore that has not been set."""

    assert _backend().signal("unknown") is None


# set_semaphore


def test_set_semaphore_stores_state():
    """Proves set_semaphore records the semaphore state by name."""

    backend = _backend()
    backend.set_semaphore("db", "impossible")
    assert backend.semaphores["db"] == "impossible"


def test_set_semaphore_overwrites_existing_state():
    """Proves set_semaphore replaces a previously stored semaphore state."""

    backend = _backend(semaphores={"db": "unsatisfied"})
    backend.set_semaphore("db", "satisfied")
    assert backend.semaphores["db"] == "satisfied"


# is_done


def test_is_done_true_when_no_pending_and_empty_queue():
    """Proves is_done returns True when pending is zero and queue is empty."""

    assert _backend(pending=0).is_done() is True


def test_is_done_false_when_pending_nonzero():
    """Proves is_done returns False when jobs are still pending."""

    assert _backend(pending=1).is_done() is False


def test_is_done_false_when_queue_nonempty():
    """Proves is_done returns False when jobs remain in the queue."""

    spec = JobSpec(fn_name="fn", args=(), reply_to=None)
    assert _backend(pending=0, queue=deque([spec])).is_done() is False


# get_error / get_result


def test_get_error_returns_none_when_no_error():
    """Proves get_error returns None when no error has been recorded."""

    assert _backend().get_error() is None


def test_get_error_returns_stored_root_error():
    """Proves get_error returns the stored root_error."""

    exc = ValueError("crash")
    assert _backend(root_error=exc).get_error() is exc


def test_get_result_returns_none_when_no_result():
    """Proves get_result returns None before any root job completes."""

    assert _backend().get_result() is None


def test_get_result_returns_stored_root_result():
    """Proves get_result returns the value stored by job_done for the root job."""

    backend = _backend(pending=1)
    backend.job_done(None, None, "final_result")
    assert backend.get_result() == "final_result"
