from collections import deque

from zahir.core.evaluate.overseer_handlers import (
    _acquire,
    _enqueue,
    _get_error,
    _get_job,
    _is_done,
    _job_done,
    _job_failed,
    _release,
    _set_semaphore,
    _signal,
)
from zahir.core.zahir_types import JobSpec, OverseerState

WORKER = b"worker-pid"


def _state(**kwargs) -> OverseerState:
    defaults = dict(queue=deque(), concurrency={}, semaphores={}, pending=0)
    return OverseerState(**{**defaults, **kwargs})


# _get_job


def test_get_job_returns_none_when_queue_empty_and_no_results():
    """Proves _get_job returns None when no jobs are queued and no results are buffered."""

    state, job = _get_job(_state(), WORKER)
    assert job is None


def test_get_job_returns_job_tuple_from_queue():
    """Proves _get_job returns a ("job", ...) tuple from the queue when no results are pending."""

    spec = JobSpec(
        fn_name="process", args=(1,), reply_to=None, timeout_ms=5000, sequence_number=3
    )
    state, job = _get_job(_state(queue=deque([spec])), WORKER)
    assert job == ("job", "process", (1,), None, 5000, 3)


def test_get_job_removes_job_from_queue():
    """Proves _get_job pops the job from the queue."""

    spec = JobSpec(fn_name="fn", args=(), reply_to=None)
    state, _ = _get_job(_state(queue=deque([spec])), WORKER)
    assert len(state.queue) == 0


def test_get_job_preserves_fifo_order():
    """Proves _get_job dequeues jobs in FIFO order."""

    specs = [JobSpec(fn_name=f"fn{i}", args=(), reply_to=None) for i in range(3)]
    state = _state(queue=deque(specs))
    _, job = _get_job(state, WORKER)
    assert job[1] == "fn0"


def test_get_job_returns_buffered_result_before_queue():
    """Proves _get_job returns a pending result for this worker before taking a new job from the queue."""

    from collections import deque as _deque

    pending = {WORKER: _deque([(42, "result")])}
    spec = JobSpec(fn_name="fn", args=(), reply_to=None)
    state, work = _get_job(_state(queue=deque([spec]), pending_results=pending), WORKER)
    assert work == ("result", 42, "result")


def test_get_job_result_is_worker_specific():
    """Proves _get_job only returns results addressed to the requesting worker."""

    from collections import deque as _deque

    other_worker = b"other-pid"
    pending = {other_worker: _deque([(1, "result")])}
    state, work = _get_job(_state(pending_results=pending), WORKER)
    assert work is None


# _enqueue


def test_enqueue_adds_job_to_queue():
    """Proves _enqueue appends a new JobSpec to the queue."""

    state = _enqueue(_state(), "process", (1,), None, None)
    assert len(state.queue) == 1
    assert state.queue[0].fn_name == "process"


def test_enqueue_increments_pending():
    """Proves _enqueue increments the pending job count."""

    state = _enqueue(_state(pending=2), "fn", (), None, None)
    assert state.pending == 3


def test_enqueue_stores_timeout_ms():
    """Proves _enqueue passes timeout_ms through to the JobSpec."""

    state = _enqueue(_state(), "fn", (), None, 3000)
    assert state.queue[0].timeout_ms == 3000


# _job_done


def test_job_done_decrements_pending():
    """Proves _job_done decrements the pending count."""

    state = _job_done(_state(pending=3), WORKER, 1, "result")
    assert state.pending == 2


def test_job_done_buffers_result_for_worker():
    """Proves _job_done stores (sequence_number, body) in pending_results for the given worker."""

    state = _job_done(_state(pending=1), WORKER, 7, "result")
    assert list(state.pending_results[WORKER]) == [(7, "result")]


def test_job_done_with_none_reply_to_skips_buffering():
    """Proves _job_done with reply_to=None (root job) only decrements pending without buffering."""

    state = _job_done(_state(pending=1), None, None, "result")
    assert state.pending == 0
    assert not state.pending_results


# _job_failed


def test_job_failed_decrements_pending():
    """Proves _job_failed decrements the pending count."""

    state = _job_failed(_state(pending=2), ValueError("boom"))
    assert state.pending == 1


def test_job_failed_stores_root_error():
    """Proves _job_failed records the error as root_error."""

    error = ValueError("boom")
    state = _job_failed(_state(pending=1), error)
    assert state.root_error is error


def test_job_failed_does_not_overwrite_existing_root_error():
    """Proves _job_failed keeps the first error and ignores subsequent ones."""

    first = ValueError("first")
    second = ValueError("second")
    state = _job_failed(_state(pending=2, root_error=first), second)
    assert state.root_error is first


# _get_error


def test_get_error_returns_none_when_no_error():
    """Proves _get_error returns None when the overseer has no stored error."""

    _, error = _get_error(_state())
    assert error is None


def test_get_error_returns_stored_root_error():
    """Proves _get_error returns the stored root_error."""

    exc = ValueError("crash")
    _, error = _get_error(_state(root_error=exc))
    assert error is exc


# _acquire


def test_acquire_grants_slot_when_under_limit():
    """Proves _acquire returns True and records the slot when under limit."""

    state, result = _acquire(_state(), "workers", 4)
    assert result is True
    assert state.concurrency["workers"] == (4, 1)


def test_acquire_denies_slot_when_at_limit():
    """Proves _acquire returns False when the concurrency limit is reached."""

    state, result = _acquire(_state(concurrency={"workers": (2, 2)}), "workers", 2)
    assert result is False


def test_acquire_respects_existing_limit():
    """Proves _acquire uses the stored limit rather than the requested one."""

    state, result = _acquire(_state(concurrency={"workers": (2, 1)}), "workers", 99)
    assert result is True
    assert state.concurrency["workers"] == (2, 2)


# _release


def test_release_decrements_active_count():
    """Proves _release decrements the active count for a known slot."""

    state = _release(_state(concurrency={"workers": (4, 3)}), "workers")
    assert state.concurrency["workers"] == (4, 2)


def test_release_does_not_go_below_zero():
    """Proves _release clamps the active count at zero."""

    state = _release(_state(concurrency={"workers": (4, 0)}), "workers")
    assert state.concurrency["workers"][1] == 0


def test_release_ignores_unknown_name():
    """Proves _release is a no-op for an unregistered slot name."""

    state = _release(_state(), "unknown")
    assert "unknown" not in state.concurrency


# _signal


def test_signal_returns_stored_state():
    """Proves _signal returns the current semaphore state."""

    state, result = _signal(_state(semaphores={"db": "satisfied"}), "db")
    assert result == "satisfied"


def test_signal_returns_none_for_unknown_name():
    """Proves _signal returns None for a semaphore that has not been set."""

    _, result = _signal(_state(), "unknown")
    assert result is None


# _set_semaphore


def test_set_semaphore_stores_state():
    """Proves _set_semaphore records the semaphore state by name."""

    state = _set_semaphore(_state(), "db", "impossible")
    assert state.semaphores["db"] == "impossible"


def test_set_semaphore_overwrites_existing_state():
    """Proves _set_semaphore replaces a previously stored semaphore state."""

    state = _set_semaphore(_state(semaphores={"db": "unsatisfied"}), "db", "satisfied")
    assert state.semaphores["db"] == "satisfied"


# _is_done


def test_is_done_true_when_no_pending_and_empty_queue():
    """Proves _is_done returns True when pending is zero and queue is empty."""

    _, result = _is_done(_state(pending=0))
    assert result is True


def test_is_done_false_when_pending_nonzero():
    """Proves _is_done returns False when jobs are still pending."""

    _, result = _is_done(_state(pending=1))
    assert result is False


def test_is_done_false_when_queue_nonempty():
    """Proves _is_done returns False when jobs remain in the queue."""

    spec = JobSpec(fn_name="fn", args=(), reply_to=None)
    _, result = _is_done(_state(pending=0, queue=deque([spec])))
    assert result is False
