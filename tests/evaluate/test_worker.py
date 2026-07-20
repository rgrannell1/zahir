import time

import pytest
from tertius import ESleep

from tests.evaluate.mocks import make_deadlined_parent
from tests.shared import drain_to
from zahir.core.commons.zahir_types import HandlerMap, JobContext
from zahir.core.effects import (
    EAcquire,
    EAwait,
    EJobComplete,
    EJobFail,
    EReleaseSlot,
    EStorageAcquire,
    EStorageRelease,
    JobSpec,
)
from zahir.core.evaluate.job_handlers import evaluate_job, make_job_handlers
from zahir.core.evaluate.suspension import RunningJob, SuspensionTable, WorkerLocals
from zahir.core.evaluate.worker import _build_job, _handle_idle, _handle_running, _Idle, _Running
from zahir.core.exceptions import JobTimeoutError


def _make_locals(acquired: list | None = None) -> WorkerLocals:
    """Build a WorkerLocals with a minimal RunningJob for evaluate_job tests."""
    acquired_slots = [] if acquired is None else acquired
    job = RunningJob(
        fn_name="test",
        eval_gen=None,
        reply_to=None,
        parent_sequence_number=None,
        acquired=acquired_slots,
    )
    return WorkerLocals(current_job=job)


def _handlers(locals_: WorkerLocals | None = None) -> HandlerMap:
    """Return pre-built job handlers for use with evaluate_job."""
    return make_job_handlers(locals_ or _make_locals(), [])


# evaluate_job — return value


def test_evaluate_job_returns_job_result():
    """Proves evaluate_job returns the value from StopIteration when the job finishes."""

    def job():
        return 42
        yield

    _, result = drain_to(evaluate_job(job(), _handlers(), None))
    assert result == 42


def test_evaluate_job_passes_through_unknown_effects():
    """Proves effects not in the handler dict are yielded to the caller unchanged."""

    unknown = ESleep(ms=100)

    def job():
        yield unknown

    effects, _ = drain_to(evaluate_job(job(), _handlers(), None))
    assert unknown in effects


# evaluate_job — deadline


def test_evaluate_job_throws_job_timeout_when_deadline_exceeded():
    """Proves evaluate_job throws JobTimeoutError into the job when the deadline passes."""

    def job():
        yield ESleep(ms=100)
        yield ESleep(ms=100)

    past_deadline = time.monotonic() - 1.0
    with pytest.raises(JobTimeoutError):
        drain_to(evaluate_job(job(), _handlers(), past_deadline))


def test_evaluate_job_job_can_catch_job_timeout():
    """Proves a job that catches JobTimeoutError can continue executing."""

    results = []

    def job():
        try:
            yield ESleep(ms=100)
        except JobTimeoutError:
            results.append("caught")

    past_deadline = time.monotonic() - 1.0
    drain_to(evaluate_job(job(), _handlers(), past_deadline))

    assert results == ["caught"]


def test_evaluate_job_no_deadline_never_times_out():
    """Proves evaluate_job with no deadline never raises JobTimeoutError."""

    def job():
        for _ in range(5):
            yield ESleep(ms=100)

    effects, _ = drain_to(evaluate_job(job(), _handlers(), None))
    assert len(effects) == 5


# _build_job — deadline construction


def test_build_job_zero_timeout_sets_immediate_deadline():
    """Proves timeout_ms=0 produces an immediate deadline rather than no deadline."""

    def zero_job(ctx):
        yield ESleep(ms=1)
        yield ESleep(ms=1)

    ctx = JobContext(_scope={"zero_job": zero_job}, scope=None)
    spec = JobSpec(fn_name="zero_job", timeout_ms=0)
    job = _build_job(spec, ctx, _handlers())

    with pytest.raises(JobTimeoutError):
        drain_to(job.eval_gen)


# _handle_idle — suspended-job expiry


def test_idle_worker_times_out_expired_suspended_parent():
    """Proves an idle pass resumes an expired suspended parent by throwing JobTimeoutError."""

    suspension = SuspensionTable()
    parent = make_deadlined_parent(deadline=time.monotonic() - 1.0)
    spec = EAwait(jobs=[JobSpec(fn_name="child")], scalar=True)
    list(suspension.suspend(spec, parent, b"me"))

    gen = _handle_idle(suspension, None, b"me", _handlers())
    next(gen)  # EGetJob
    with pytest.raises(StopIteration) as exc:
        gen.send(None)

    state = exc.value.value
    assert isinstance(state, _Running)
    assert state.job.fn_name == "parent"
    assert isinstance(state.pending_throw, JobTimeoutError)


def test_idle_worker_leaves_live_suspended_parents_alone():
    """Proves an idle pass with no expired parents stays idle."""

    suspension = SuspensionTable()
    parent = make_deadlined_parent(deadline=time.monotonic() + 60.0)
    spec = EAwait(jobs=[JobSpec(fn_name="child")], scalar=True)
    list(suspension.suspend(spec, parent, b"me"))

    gen = _handle_idle(suspension, None, b"me", _handlers())
    next(gen)  # EGetJob
    with pytest.raises(StopIteration) as exc:
        gen.send(None)

    assert isinstance(exc.value.value, _Idle)


# evaluate_job — acquired tracking


def _drive_with_acquire_result(gen, granted: bool):
    """Drive evaluate_job, intercepting EStorageAcquire and feeding back the granted value."""
    effects, _value = [], None
    try:
        effect = next(gen)
        while True:
            if isinstance(effect, EStorageAcquire):
                effect = gen.send(granted)
            else:
                effects.append(effect)
                effect = gen.send(None)
    except StopIteration as exc:
        return effects, exc.value


def test_evaluate_job_tracks_acquired_slots():
    """Proves evaluate_job populates acquired list via the acquire handler."""

    acquired = []
    locals_ = _make_locals(acquired)

    def job():
        yield EAcquire(name="workers", limit=4)

    job_gen = evaluate_job(job(), make_job_handlers(locals_, []), None)
    _drive_with_acquire_result(job_gen, granted=True)
    assert acquired == ["workers"]


def test_evaluate_job_does_not_track_denied_slots():
    """Proves evaluate_job does not add to acquired when the slot is denied."""

    acquired = []
    locals_ = _make_locals(acquired)

    def job():
        yield EAcquire(name="workers", limit=4)

    job_gen = evaluate_job(job(), make_job_handlers(locals_, []), None)
    _drive_with_acquire_result(job_gen, granted=False)
    assert acquired == []


def test_release_slot_frees_slot_before_job_exit():
    """Proves EReleaseSlot releases the slot immediately and removes it from exit cleanup."""

    acquired = []
    locals_ = _make_locals(acquired)

    def job():
        yield EAcquire(name="gate", limit=1)
        yield EReleaseSlot(name="gate")

    job_gen = evaluate_job(job(), make_job_handlers(locals_, []), None)
    effects, _ = _drive_with_acquire_result(job_gen, granted=True)

    assert acquired == []
    releases = [effect for effect in effects if isinstance(effect, EStorageRelease)]
    assert [release.name for release in releases] == ["gate"]


def test_release_slot_of_unheld_slot_is_a_noop():
    """Proves EReleaseSlot for a slot the job never acquired releases nothing."""

    locals_ = _make_locals([])

    def job():
        yield EReleaseSlot(name="never-acquired")

    job_gen = evaluate_job(job(), make_job_handlers(locals_, []), None)
    effects, _ = drain_to(job_gen)

    assert not any(isinstance(effect, EStorageRelease) for effect in effects)


# _handle_running — slot release on failure


def _make_running(job_fn, acquired: list[str]) -> tuple[_Running, WorkerLocals]:
    """Build a _Running state with pre-populated acquired slots for a job that will fail."""
    locals_ = _make_locals(acquired)
    running_job = RunningJob(
        fn_name="test",
        eval_gen=evaluate_job(job_fn(), _handlers(locals_), None),
        reply_to=None,
        parent_sequence_number=None,
        acquired=acquired,
    )
    return _Running(job=running_job), locals_


def test_slots_released_on_unhandled_exception():
    """Proves _handle_running releases all acquired slots when the job raises."""

    def failing_job():
        raise RuntimeError("boom")
        yield

    state, locals_ = _make_running(failing_job, ["workers", "io"])
    effects, _ = drain_to(_handle_running(state, locals_))

    released = {e.name for e in effects if isinstance(e, EStorageRelease)}
    assert released == {"workers", "io"}


def test_slots_released_before_job_fail_reported():
    """Proves EStorageRelease effects all precede EJobFail in the output stream."""

    def failing_job():
        raise RuntimeError("boom")
        yield

    state, locals_ = _make_running(failing_job, ["slot-a", "slot-b"])
    effects, _ = drain_to(_handle_running(state, locals_))

    indices = {type(e): idx for idx, e in enumerate(effects)}
    release_indices = [idx for idx, e in enumerate(effects) if isinstance(e, EStorageRelease)]
    fail_idx = indices[EJobFail]
    assert all(idx < fail_idx for idx in release_indices)


def test_no_slots_held_job_fail_still_reported():
    """Proves EJobFail is still emitted when the job holds no concurrency slots."""

    def failing_job():
        raise RuntimeError("boom")
        yield

    state, locals_ = _make_running(failing_job, [])
    effects, _ = drain_to(_handle_running(state, locals_))

    assert any(isinstance(e, EJobFail) for e in effects)
    assert not any(isinstance(e, EStorageRelease) for e in effects)


# _handle_running — worker-level handler failures


def test_handler_error_during_effect_is_thrown_into_job():
    """Proves a worker-level handler failure is thrown into the job, not out of the worker."""

    def sleeping_job():
        yield ESleep(ms=1)

    state, locals_ = _make_running(sleeping_job, [])
    gen = _handle_running(state, locals_)
    next(gen)  # the ESleep effect, yielded to worker-level handlers

    boom = RuntimeError("handler exploded")
    with pytest.raises(StopIteration) as exc:
        gen.throw(boom)

    next_state = exc.value.value
    assert isinstance(next_state, _Running)
    assert next_state.pending_throw is boom


def test_failed_completion_report_reports_failure_instead():
    """Proves a failing EJobComplete handler degrades to EJobFail rather than a dead worker."""

    def finishing_job():
        return "unpicklable-result"
        yield

    state, locals_ = _make_running(finishing_job, [])
    gen = _handle_running(state, locals_)

    effect = next(gen)  # EJobComplete
    assert isinstance(effect, EJobComplete)

    effect = gen.throw(RuntimeError("cannot pickle result"))
    assert isinstance(effect, EJobFail)

    with pytest.raises(StopIteration) as exc:
        gen.send(None)
    assert isinstance(exc.value.value, _Idle)


# evaluate_job — unhandled exceptions


def test_evaluate_job_propagates_unhandled_exception():
    """Proves evaluate_job lets non-JobTimeoutError exceptions propagate to the caller."""

    def job():
        raise ValueError("unexpected")
        yield

    with pytest.raises(ValueError, match="unexpected"):
        drain_to(evaluate_job(job(), _handlers(), None))
