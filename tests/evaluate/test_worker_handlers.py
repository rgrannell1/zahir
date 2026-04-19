from unittest.mock import patch

import pytest

from tertius import EReceive, ESelf, Pid
from tertius.types import Envelope

from constants import ACQUIRE, ENQUEUE, RELEASE, SIGNAL, SET_SEMAPHORE
from effects import (
    EAcquire,
    EAwait,
    EAwaitAll,
    EImpossible,
    ESatisfied,
    ESetSemaphore,
    ESignal,
)
from exceptions import JobError
from evaluate.worker_handlers import (
    _handle_acquire,
    _handle_await,
    _handle_await_all,
    _handle_event,
    _handle_set_semaphore,
    _handle_signal,
    make_handlers,
)
from exceptions import JobTimeout


OVERSEER = Pid(id=1)
ME = Pid(id=2)


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


# _handle_event


def test_handle_event_returns_effect_for_satisfied():
    """Proves _handle_event returns the ESatisfied effect for job introspection."""

    gen = _handle_event(ESatisfied())
    with pytest.raises(StopIteration) as exc:
        next(gen)
    assert exc.value.value == ESatisfied()


def test_handle_event_returns_effect_for_impossible():
    """Proves _handle_event returns the EImpossible effect for job introspection."""

    gen = _handle_event(EImpossible(reason="blocked"))
    with pytest.raises(StopIteration) as exc:
        next(gen)
    assert exc.value.value == EImpossible(reason="blocked")


# _handle_await


def test_handle_await_first_yields_eself():
    """Proves _handle_await first requests its own Pid via ESelf."""

    with patch("evaluate.worker_handlers.mcast", _mock_mcast()):
        gen = _handle_await(OVERSEER, EAwait(fn_name="child"))
        assert isinstance(next(gen), ESelf)


def test_handle_await_enqueues_job_after_self():
    """Proves _handle_await mcasts an enqueue message with fn_name and args."""

    sent = []

    def _capturing_mcast(pid, body):
        sent.append((pid, body))
        return None
        yield

    with patch("evaluate.worker_handlers.mcast", _capturing_mcast):
        gen = _handle_await(
            OVERSEER, EAwait(fn_name="child", args=(1,), timeout_ms=500)
        )
        next(gen)  # ESelf
        effect = gen.send(ME)  # mcast → ESend, then EReceive
        assert isinstance(effect, EReceive)

    assert sent[0][0] == OVERSEER
    assert sent[0][1][0] == ENQUEUE
    assert sent[0][1][1] == "child"
    assert sent[0][1][4] == 500


def test_handle_await_returns_envelope_body():
    """Proves _handle_await returns the body of the received envelope."""

    with patch("evaluate.worker_handlers.mcast", _mock_mcast()):
        gen = _handle_await(OVERSEER, EAwait(fn_name="child"))
        next(gen)
        gen.send(ME)
        envelope = Envelope(sender=OVERSEER, body=(None, "result"))
        with pytest.raises(StopIteration) as exc:
            gen.send(envelope)
        assert exc.value.value == "result"


def test_handle_await_raises_job_timeout_on_timeout():
    """Proves _handle_await raises JobTimeout when a JobTimeout is received as the body."""

    with patch("evaluate.worker_handlers.mcast", _mock_mcast()):
        gen = _handle_await(OVERSEER, EAwait(fn_name="child", timeout_ms=1000))
        next(gen)
        gen.send(ME)
        envelope = Envelope(sender=OVERSEER, body=(None, JobTimeout()))
        with pytest.raises(JobTimeout):
            gen.send(envelope)


# _handle_acquire


def test_handle_acquire_returns_true_and_tracks_name():
    """Proves _handle_acquire appends the name to acquired when slot is granted."""

    acquired = []
    with patch("evaluate.worker_handlers.mcall", _mock_mcall(True)):
        gen = _handle_acquire(OVERSEER, acquired, EAcquire(name="workers", limit=4))
        with pytest.raises(StopIteration) as exc:
            next(gen)
        assert exc.value.value is True
    assert acquired == ["workers"]


def test_handle_acquire_returns_false_and_does_not_track():
    """Proves _handle_acquire does not append to acquired when slot is denied."""

    acquired = []
    with patch("evaluate.worker_handlers.mcall", _mock_mcall(False)):
        gen = _handle_acquire(OVERSEER, acquired, EAcquire(name="workers", limit=4))
        with pytest.raises(StopIteration) as exc:
            next(gen)
        assert exc.value.value is False
    assert acquired == []


# _handle_signal


def test_handle_signal_returns_semaphore_state():
    """Proves _handle_signal returns the state string from the overseer."""

    with patch("evaluate.worker_handlers.mcall", _mock_mcall("satisfied")):
        gen = _handle_signal(OVERSEER, ESignal(name="db"))
        with pytest.raises(StopIteration) as exc:
            next(gen)
        assert exc.value.value == "satisfied"


# _handle_set_semaphore


def test_handle_set_semaphore_mcasts_correct_message():
    """Proves _handle_set_semaphore sends the semaphore name and state to the overseer."""

    sent = []

    def _capturing(pid, body):
        sent.append((pid, body))
        return None
        yield

    with patch("evaluate.worker_handlers.mcast", _capturing):
        gen = _handle_set_semaphore(
            OVERSEER, ESetSemaphore(name="db", state="impossible")
        )
        with pytest.raises(StopIteration):
            next(gen)

    assert sent[0] == (OVERSEER, (SET_SEMAPHORE, "db", "impossible"))


# _handle_await_all


def _drive_await_all(effects, envelopes):
    """Drive _handle_await_all to completion, feeding in envelopes in sequence."""
    with patch("evaluate.worker_handlers.mcast", _mock_mcast()):
        gen = _handle_await_all(OVERSEER, EAwaitAll(effects=effects))
        next(gen)           # ESelf
        gen.send(ME)        # triggers mcast calls, lands on first EReceive
        for i, env in enumerate(envelopes):
            if i < len(envelopes) - 1:
                gen.send(env)   # EReceive
            else:
                with pytest.raises(StopIteration) as exc:
                    gen.send(env)
                return exc.value.value
    return None


def test_handle_await_all_returns_results_in_input_order():
    """Proves _handle_await_all returns results ordered by input position, not arrival order."""

    effects = [EAwait(fn_name="a"), EAwait(fn_name="b")]
    # replies arrive in reverse order: b first, then a
    envelopes = [
        Envelope(sender=OVERSEER, body=(1, "result_b")),
        Envelope(sender=OVERSEER, body=(0, "result_a")),
    ]
    result = _drive_await_all(effects, envelopes)
    assert result == ["result_a", "result_b"]


def test_handle_await_all_raises_job_error_when_child_fails():
    """Proves _handle_await_all raises JobError when a child job returns an error."""

    effects = [EAwait(fn_name="a"), EAwait(fn_name="b")]
    error = JobError(ValueError("boom"))
    envelopes = [
        Envelope(sender=OVERSEER, body=(0, error)),
        Envelope(sender=OVERSEER, body=(1, "ok")),
    ]
    with patch("evaluate.worker_handlers.mcast", _mock_mcast()):
        gen = _handle_await_all(OVERSEER, EAwaitAll(effects=effects))
        next(gen)
        gen.send(ME)
        gen.send(envelopes[0])
        with pytest.raises(JobError):
            gen.send(envelopes[1])


def test_handle_await_all_raises_job_timeout_when_child_times_out():
    """Proves _handle_await_all raises JobTimeout when a child job times out."""

    effects = [EAwait(fn_name="a"), EAwait(fn_name="b")]
    envelopes = [
        Envelope(sender=OVERSEER, body=(0, JobTimeout())),
        Envelope(sender=OVERSEER, body=(1, "ok")),
    ]
    with patch("evaluate.worker_handlers.mcast", _mock_mcast()):
        gen = _handle_await_all(OVERSEER, EAwaitAll(effects=effects))
        next(gen)
        gen.send(ME)
        gen.send(envelopes[0])
        with pytest.raises(JobTimeout):
            gen.send(envelopes[1])


def test_handle_await_all_drains_all_replies_before_raising():
    """Proves _handle_await_all collects all N replies before raising the first error."""

    effects = [EAwait(fn_name="a"), EAwait(fn_name="b"), EAwait(fn_name="c")]
    error = JobError(ValueError("first"))
    with patch("evaluate.worker_handlers.mcast", _mock_mcast()):
        gen = _handle_await_all(OVERSEER, EAwaitAll(effects=effects))
        next(gen)
        gen.send(ME)
        gen.send(Envelope(sender=OVERSEER, body=(0, error)))   # first error
        gen.send(Envelope(sender=OVERSEER, body=(1, "ok")))    # normal result
        with pytest.raises(JobError):
            gen.send(Envelope(sender=OVERSEER, body=(2, "ok")))  # third reply triggers raise


def test_handle_await_all_raises_first_error_when_multiple_fail():
    """Proves _handle_await_all raises the first error encountered, not the last."""

    effects = [EAwait(fn_name="a"), EAwait(fn_name="b")]
    first_error = JobError(ValueError("first"))
    second_error = JobError(ValueError("second"))
    with patch("evaluate.worker_handlers.mcast", _mock_mcast()):
        gen = _handle_await_all(OVERSEER, EAwaitAll(effects=effects))
        next(gen)
        gen.send(ME)
        gen.send(Envelope(sender=OVERSEER, body=(0, first_error)))
        with pytest.raises(JobError) as exc:
            gen.send(Envelope(sender=OVERSEER, body=(1, second_error)))
    assert exc.value is first_error


# make_handlers


def test_make_handlers_contains_all_effect_types():
    """Proves make_handlers returns entries for all handled effect types."""

    handlers = make_handlers(OVERSEER, [])
    assert set(handlers.keys()) == {
        ESatisfied,
        EImpossible,
        EAwait,
        EAwaitAll,
        EAcquire,
        ESignal,
        ESetSemaphore,
    }


def test_make_handlers_returns_callables():
    """Proves every handler value in make_handlers is callable."""

    handlers = make_handlers(OVERSEER, [])
    assert all(callable(h) for h in handlers.values())
