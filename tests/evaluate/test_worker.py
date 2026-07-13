from datetime import timedelta

import pytest
import time_machine
from tertius import ESleep

from tests.shared import NOW, drain_to
from zahir.core.effects import EAcquire, EAcquireSlot, EJobFail, ERelease
from zahir.core.evaluate.job_handlers import evaluate_job, make_job_handlers
from zahir.core.evaluate.suspension import RunningJob, WorkerLocals
from zahir.core.evaluate.worker import _handle_running, _Running
from zahir.core.exceptions import JobTimeoutError
from zahir.core.zahir_types import JobHandlerMap


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


def _handlers(locals_: WorkerLocals | None = None) -> JobHandlerMap:
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

    with time_machine.travel(NOW, tick=False):
        past_deadline = NOW - timedelta(seconds=1)
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

    with time_machine.travel(NOW, tick=False):
        past_deadline = NOW - timedelta(seconds=1)
        drain_to(evaluate_job(job(), _handlers(), past_deadline))

    assert results == ["caught"]


def test_evaluate_job_no_deadline_never_times_out():
    """Proves evaluate_job with no deadline never raises JobTimeoutError."""

    def job():
        for _ in range(5):
            yield ESleep(ms=100)

    effects, _ = drain_to(evaluate_job(job(), _handlers(), None))
    assert len(effects) == 5


# evaluate_job — acquired tracking


def _drive_with_acquire_result(gen, granted: bool):
    """Drive evaluate_job, intercepting EAcquireSlot and feeding back the granted value."""
    effects, _value = [], None
    try:
        effect = next(gen)
        while True:
            if isinstance(effect, EAcquireSlot):
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

    released = {e.name for e in effects if isinstance(e, ERelease)}
    assert released == {"workers", "io"}


def test_slots_released_before_job_fail_reported():
    """Proves ERelease effects all precede EJobFail in the output stream."""

    def failing_job():
        raise RuntimeError("boom")
        yield

    state, locals_ = _make_running(failing_job, ["slot-a", "slot-b"])
    effects, _ = drain_to(_handle_running(state, locals_))

    indices = {type(e): idx for idx, e in enumerate(effects)}
    release_indices = [idx for idx, e in enumerate(effects) if isinstance(e, ERelease)]
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
    assert not any(isinstance(e, ERelease) for e in effects)


# evaluate_job — unhandled exceptions


def test_evaluate_job_propagates_unhandled_exception():
    """Proves evaluate_job lets non-JobTimeoutError exceptions propagate to the caller."""

    def job():
        raise ValueError("unexpected")
        yield

    with pytest.raises(ValueError, match="unexpected"):
        drain_to(evaluate_job(job(), _handlers(), None))
