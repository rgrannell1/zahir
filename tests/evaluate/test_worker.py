from datetime import timedelta
import pytest
import time_machine

from tertius import ESleep

from zahir.core.effects import EAcquire, EAcquireSlot
from zahir.core.evaluate.job_handlers import JobHandlerContext
from zahir.core.evaluate.job_handlers import evaluate_job
from zahir.core.exceptions import JobError, JobTimeout
from tests.evaluate.mocks import OVERSEER
from tests.shared import NOW


def _drive(gen):
    effects, value = [], None
    try:
        effect = next(gen)
        while True:
            effects.append(effect)
            effect = gen.send(None)
    except StopIteration as exc:
        return effects, exc.value


# evaluate_job — return value


def test_evaluate_job_returns_job_result():
    """Proves evaluate_job returns the value from StopIteration when the job finishes."""

    def job():
        return 42
        yield

    _, result = _drive(evaluate_job(job(), JobHandlerContext(), None))
    assert result == 42


def test_evaluate_job_passes_through_unknown_effects():
    """Proves effects not in the handler dict are yielded to the caller unchanged."""

    unknown = ESleep(ms=100)

    def job():
        yield unknown

    effects, _ = _drive(evaluate_job(job(), JobHandlerContext(), None))
    assert unknown in effects


# evaluate_job — deadline


def test_evaluate_job_throws_job_timeout_when_deadline_exceeded():
    """Proves evaluate_job throws JobTimeout into the job when the deadline passes."""

    def job():
        yield ESleep(ms=100)
        yield ESleep(ms=100)

    with time_machine.travel(NOW, tick=False):
        past_deadline = NOW - timedelta(seconds=1)
        with pytest.raises(JobTimeout):
            _drive(evaluate_job(job(), JobHandlerContext(), past_deadline))


def test_evaluate_job_job_can_catch_job_timeout():
    """Proves a job that catches JobTimeout can continue executing."""

    results = []

    def job():
        try:
            yield ESleep(ms=100)
        except JobTimeout:
            results.append("caught")

    with time_machine.travel(NOW, tick=False):
        past_deadline = NOW - timedelta(seconds=1)
        _drive(evaluate_job(job(), JobHandlerContext(), past_deadline))

    assert results == ["caught"]


def test_evaluate_job_no_deadline_never_times_out():
    """Proves evaluate_job with no deadline never raises JobTimeout."""

    def job():
        for _ in range(5):
            yield ESleep(ms=100)

    effects, _ = _drive(evaluate_job(job(), JobHandlerContext(), None))
    assert len(effects) == 5


# evaluate_job — acquired tracking


def _drive_with_acquire_result(gen, granted: bool):
    """Drive evaluate_job, intercepting EAcquireSlot and feeding back the granted value."""
    effects, value = [], None
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
    ctx = JobHandlerContext(acquired=acquired)

    def job():
        yield EAcquire(name="workers", limit=4)

    _drive_with_acquire_result(evaluate_job(job(), ctx, None), granted=True)
    assert acquired == ["workers"]


def test_evaluate_job_does_not_track_denied_slots():
    """Proves evaluate_job does not add to acquired when the slot is denied."""

    acquired = []
    ctx = JobHandlerContext(acquired=acquired)

    def job():
        yield EAcquire(name="workers", limit=4)

    _drive_with_acquire_result(evaluate_job(job(), ctx, None), granted=False)
    assert acquired == []


# evaluate_job — unhandled exceptions


def test_evaluate_job_propagates_unhandled_exception():
    """Proves evaluate_job lets non-JobTimeout exceptions propagate to the caller."""

    def job():
        raise ValueError("unexpected")
        yield

    with pytest.raises(ValueError, match="unexpected"):
        _drive(evaluate_job(job(), JobHandlerContext(), None))
