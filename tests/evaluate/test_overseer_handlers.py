# Tests for MemoryBackend methods — proves each storage operation behaves correctly in isolation.
from collections import deque

from zahir.core.backends.memory import MemoryBackend
from zahir.core.zahir_types import JobSpec, ResultItem

WORKER = b"worker-pid"


def _backend(**kwargs) -> MemoryBackend:
    return MemoryBackend(**kwargs)


# get_job


def test_get_job_returns_none_when_queue_empty_and_no_results():
    """Proves get_job returns None when no jobs are queued and no results are buffered."""

    assert _backend().get_job(WORKER, None) is None


def test_get_job_returns_leased_job_spec_from_queue():
    """Proves get_job returns the queued JobSpec itself, wrapped in a lease."""

    spec = JobSpec(fn_name="process", args=(1,), reply_to=None, timeout_ms=5000, sequence_number=3)
    lease_id, work = _backend(queue=deque([spec])).get_job(WORKER, None)
    assert work is spec
    assert lease_id == 1


def test_get_job_removes_job_from_queue():
    """Proves get_job pops the job from the queue."""

    spec = JobSpec(fn_name="fn", args=(), reply_to=None)
    backend = _backend(queue=deque([spec]))
    backend.get_job(WORKER, None)
    assert len(backend.queue) == 0


def test_get_job_preserves_fifo_order():
    """Proves get_job dequeues jobs in FIFO order."""

    specs = [JobSpec(fn_name=f"fn{idx}", args=(), reply_to=None) for idx in range(3)]
    _, work = _backend(queue=deque(specs)).get_job(WORKER, None)
    assert work.fn_name == "fn0"


def test_get_job_returns_buffered_result_before_queue():
    """Proves get_job returns pending result for this worker before a new job."""

    pending = {WORKER: deque([(42, "result")])}
    spec = JobSpec(fn_name="fn", args=(), reply_to=None)
    _, work = _backend(queue=deque([spec]), pending_results=pending).get_job(WORKER, None)
    assert work == ResultItem(sequence_number=42, body="result")


def test_get_job_result_is_worker_specific():
    """Proves get_job only returns results addressed to the requesting worker."""

    other_worker = b"other-pid"
    pending = {other_worker: deque([(1, "result")])}
    assert _backend(pending_results=pending).get_job(WORKER, None) is None


# get_job — lease protocol


def test_get_job_redelivers_work_when_reply_was_lost():
    """Proves work handed to a worker whose reply timed out is re-delivered, not lost."""

    spec = JobSpec(fn_name="fn", args=(), reply_to=None)
    backend = _backend(queue=deque([spec]))

    first = backend.get_job(WORKER, None)
    second = backend.get_job(WORKER, None)
    assert second == first


def test_get_job_ack_clears_lease_and_hands_out_next_item():
    """Proves an acked lease is dropped and the next queued item is delivered."""

    specs = [JobSpec(fn_name=f"fn{idx}", args=(), reply_to=None) for idx in range(2)]
    backend = _backend(queue=deque(specs))

    first_lease_id, first_work = backend.get_job(WORKER, None)
    second_lease_id, second_work = backend.get_job(WORKER, first_lease_id)
    assert first_work.fn_name == "fn0"
    assert second_work.fn_name == "fn1"
    assert second_lease_id != first_lease_id


def test_get_job_ack_is_processed_when_queue_is_empty():
    """Proves an ack clears the lease even with no new work, so it is never re-delivered."""

    spec = JobSpec(fn_name="fn", args=(), reply_to=None)
    backend = _backend(queue=deque([spec]))

    lease_id, _ = backend.get_job(WORKER, None)
    assert backend.get_job(WORKER, lease_id) is None
    assert backend.get_job(WORKER, None) is None


def test_get_job_stale_ack_still_redelivers_current_lease():
    """Proves an ack for an older lease does not clear the current one."""

    specs = [JobSpec(fn_name=f"fn{idx}", args=(), reply_to=None) for idx in range(2)]
    backend = _backend(queue=deque(specs))

    first_lease_id, _ = backend.get_job(WORKER, None)
    current = backend.get_job(WORKER, first_lease_id)
    redelivered = backend.get_job(WORKER, first_lease_id)
    assert redelivered == current


def test_get_job_leases_are_worker_specific():
    """Proves one worker's un-acked lease does not affect another worker's handout."""

    other_worker = b"other-pid"
    specs = [JobSpec(fn_name=f"fn{idx}", args=(), reply_to=None) for idx in range(2)]
    backend = _backend(queue=deque(specs))

    _, first_work = backend.get_job(WORKER, None)
    _, second_work = backend.get_job(other_worker, None)
    assert first_work.fn_name == "fn0"
    assert second_work.fn_name == "fn1"


def test_get_job_redelivers_buffered_results_too():
    """Proves un-acked buffered results are re-delivered like jobs."""

    pending = {WORKER: deque([(42, "result")])}
    backend = _backend(pending_results=pending)

    first = backend.get_job(WORKER, None)
    second = backend.get_job(WORKER, None)
    assert second == first


# enqueue


def test_enqueue_adds_job_to_queue():
    """Proves enqueue appends a new JobSpec to the queue."""

    backend = _backend()
    backend.enqueue(JobSpec(fn_name="process", args=(1,), reply_to=None))
    assert len(backend.queue) == 1
    assert backend.queue[0].fn_name == "process"


def test_enqueue_increments_pending():
    """Proves enqueue increments the pending job count."""

    backend = _backend(pending=2)
    backend.enqueue(JobSpec(fn_name="fn", args=(), reply_to=None))
    assert backend.pending == 3


def test_enqueue_stores_timeout_ms():
    """Proves enqueue passes timeout_ms through to the JobSpec."""

    backend = _backend()
    backend.enqueue(JobSpec(fn_name="fn", args=(), reply_to=None, timeout_ms=3000))
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


# get_state


def test_get_state_returns_stored_state():
    """Proves get_state returns the current KV state."""

    assert _backend(state={"db": "satisfied"}).get_state("db") == "satisfied"


def test_get_state_returns_none_for_unknown_name():
    """Proves get_state returns None for a name that has not been set."""

    assert _backend().get_state("unknown") is None


# set_state


def test_set_state_stores_state():
    """Proves set_state records the KV state by name."""

    backend = _backend()
    backend.set_state("db", "impossible")
    assert backend.state["db"] == "impossible"


def test_set_state_overwrites_existing_state():
    """Proves set_state replaces a previously stored value."""

    backend = _backend(state={"db": "unsatisfied"})
    backend.set_state("db", "satisfied")
    assert backend.state["db"] == "satisfied"


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
