from datetime import UTC, datetime, timedelta
from unittest.mock import patch

import pytest
import time_machine

from tertius import ESleep, Pid
from tertius.types import Envelope

from effects import EAcquire, EImpossible, ESatisfied, ESignal
from evaluate.worker import evaluate_job
from evaluate.worker_handlers import _TIMEOUT_SENTINEL
from exceptions import JobTimeout


OVERSEER = Pid(id=1)
NOW = datetime(2025, 1, 1, 12, 0, 0, tzinfo=UTC)


def _mock_mcall(return_value):
    def _gen(pid, body):
        return return_value
        yield
    return _gen


def _mock_mcast():
    def _gen(pid, body):
        return None
        yield
    return _gen


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

    _, result = _drive(evaluate_job(job(), OVERSEER, [], None))
    assert result == 42


def test_evaluate_job_passes_through_unknown_effects():
    """Proves effects not in the handler dict are yielded to the caller unchanged."""

    unknown = ESleep(ms=100)

    def job():
        yield unknown

    effects, _ = _drive(evaluate_job(job(), OVERSEER, [], None))
    assert unknown in effects


# evaluate_job — handled effects

def test_evaluate_job_intercepts_esatisfied():
    """Proves ESatisfied is intercepted and None is sent back to the job."""

    received = []

    def job():
        val = yield ESatisfied()
        received.append(val)

    _drive(evaluate_job(job(), OVERSEER, [], None))
    assert received == [None]


def test_evaluate_job_intercepts_eimpossible():
    """Proves EImpossible is intercepted and None is sent back to the job."""

    received = []

    def job():
        val = yield EImpossible(reason="blocked")
        received.append(val)

    _drive(evaluate_job(job(), OVERSEER, [], None))
    assert received == [None]


# evaluate_job — deadline

def test_evaluate_job_throws_job_timeout_when_deadline_exceeded():
    """Proves evaluate_job throws JobTimeout into the job when the deadline passes."""

    def job():
        yield ESleep(ms=100)
        yield ESleep(ms=100)

    with time_machine.travel(NOW, tick=False):
        past_deadline = NOW - timedelta(seconds=1)
        with pytest.raises(JobTimeout):
            _drive(evaluate_job(job(), OVERSEER, [], past_deadline))


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
        _drive(evaluate_job(job(), OVERSEER, [], past_deadline))

    assert results == ["caught"]


def test_evaluate_job_no_deadline_never_times_out():
    """Proves evaluate_job with no deadline never raises JobTimeout."""

    def job():
        for _ in range(5):
            yield ESleep(ms=100)

    effects, _ = _drive(evaluate_job(job(), OVERSEER, [], None))
    assert len(effects) == 5


# evaluate_job — acquired tracking

def test_evaluate_job_tracks_acquired_slots():
    """Proves evaluate_job populates acquired list via the acquire handler."""

    acquired = []

    with patch("evaluate.worker_handlers.mcall", _mock_mcall(True)):
        def job():
            yield EAcquire(name="workers", limit=4)

        _drive(evaluate_job(job(), OVERSEER, acquired, None))

    assert acquired == ["workers"]


def test_evaluate_job_does_not_track_denied_slots():
    """Proves evaluate_job does not add to acquired when the slot is denied."""

    acquired = []

    with patch("evaluate.worker_handlers.mcall", _mock_mcall(False)):
        def job():
            yield EAcquire(name="workers", limit=4)

        _drive(evaluate_job(job(), OVERSEER, acquired, None))

    assert acquired == []
