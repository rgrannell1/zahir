from unittest.mock import patch

import pytest

from tertius import EReceive
from tertius.types import Envelope

from constants import ACQUIRE, SET_SEMAPHORE, SIGNAL
from effects import (
    EAcquire,
    EAwait,
    EAwaitAll,
    EEnqueue,
    EImpossible,
    ESatisfied,
    ESetSemaphore,
    ESignal,
)
from evaluate.job_handlers import (
    JobHandlerContext,
    _handle_acquire,
    _handle_await,
    _handle_await_all,
    _handle_event,
    _handle_set_semaphore,
    _handle_signal,
    make_job_handlers,
)
from exceptions import JobError, JobTimeout
from tests.evaluate.mocks import OVERSEER, mock_mcall

CTX = JobHandlerContext(overseer=OVERSEER)


# _handle_event


def test_handle_event_returns_effect_for_satisfied():
    """Proves _handle_event returns the ESatisfied effect for job introspection."""

    gen = _handle_event(CTX, ESatisfied())
    with pytest.raises(StopIteration) as exc:
        next(gen)
    assert exc.value.value == ESatisfied()


def test_handle_event_returns_effect_for_impossible():
    """Proves _handle_event returns the EImpossible effect for job introspection."""

    gen = _handle_event(CTX, EImpossible(reason="blocked"))
    with pytest.raises(StopIteration) as exc:
        next(gen)
    assert exc.value.value == EImpossible(reason="blocked")


# _handle_await


def test_handle_await_first_yields_eenqueue():
    """Proves _handle_await first yields EEnqueue to dispatch the child job."""

    gen = _handle_await(CTX, EAwait(fn_name="child"))
    assert isinstance(next(gen), EEnqueue)


def test_handle_await_eenqueue_carries_correct_fields():
    """Proves _handle_await yields EEnqueue with the correct fn_name, args, timeout_ms, and nonce."""

    gen = _handle_await(CTX, EAwait(fn_name="child", args=(1,), timeout_ms=500))
    enqueue = next(gen)
    assert enqueue.fn_name == "child"
    assert enqueue.args == (1,)
    assert enqueue.timeout_ms == 500
    assert enqueue.nonce is None


def test_handle_await_yields_ereceive_after_enqueue():
    """Proves _handle_await yields EReceive after EEnqueue to wait for the reply."""

    gen = _handle_await(CTX, EAwait(fn_name="child"))
    next(gen)                        # EEnqueue
    assert isinstance(gen.send(None), EReceive)


def test_handle_await_returns_envelope_body():
    """Proves _handle_await returns the body of the received envelope."""

    gen = _handle_await(CTX, EAwait(fn_name="child"))
    next(gen)                        # EEnqueue
    gen.send(None)                   # EReceive
    envelope = Envelope(sender=OVERSEER, body=(None, "result"))
    with pytest.raises(StopIteration) as exc:
        gen.send(envelope)
    assert exc.value.value == "result"


def test_handle_await_raises_job_timeout_on_timeout():
    """Proves _handle_await raises JobTimeout when a JobTimeout is received as the body."""

    gen = _handle_await(CTX, EAwait(fn_name="child", timeout_ms=1000))
    next(gen)                        # EEnqueue
    gen.send(None)                   # EReceive
    envelope = Envelope(sender=OVERSEER, body=(None, JobTimeout()))
    with pytest.raises(JobTimeout):
        gen.send(envelope)


# _handle_acquire


def test_handle_acquire_returns_true_and_tracks_name():
    """Proves _handle_acquire appends the name to acquired when slot is granted."""

    acquired = []
    with patch("evaluate.job_handlers.mcall", mock_mcall(True)):
        gen = _handle_acquire(JobHandlerContext(overseer=OVERSEER, acquired=acquired), EAcquire(name="workers", limit=4))
        with pytest.raises(StopIteration) as exc:
            next(gen)
        assert exc.value.value is True
    assert acquired == ["workers"]


def test_handle_acquire_returns_false_and_does_not_track():
    """Proves _handle_acquire does not append to acquired when slot is denied."""

    acquired = []
    with patch("evaluate.job_handlers.mcall", mock_mcall(False)):
        gen = _handle_acquire(JobHandlerContext(overseer=OVERSEER, acquired=acquired), EAcquire(name="workers", limit=4))
        with pytest.raises(StopIteration) as exc:
            next(gen)
        assert exc.value.value is False
    assert acquired == []


# _handle_signal


def test_handle_signal_returns_semaphore_state():
    """Proves _handle_signal returns the state string from the overseer."""

    with patch("evaluate.job_handlers.mcall", mock_mcall("satisfied")):
        gen = _handle_signal(CTX, ESignal(name="db"))
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

    with patch("evaluate.job_handlers.mcast", _capturing):
        gen = _handle_set_semaphore(
            CTX, ESetSemaphore(name="db", state="impossible")
        )
        with pytest.raises(StopIteration):
            next(gen)

    assert sent[0] == (OVERSEER, (SET_SEMAPHORE, "db", "impossible"))


# _handle_await_all


def _drive_await_all(effects, envelopes):
    """Drive _handle_await_all to completion, feeding in envelopes in sequence."""
    gen = _handle_await_all(CTX, EAwaitAll(effects=effects))
    # next() starts the generator and yields the first EEnqueue;
    # each subsequent send(None) consumes the remaining EEnqueues then lands on the first EReceive
    next(gen)
    for _ in effects:
        gen.send(None)
    # now feed envelopes into EReceive yields
    for idx, env in enumerate(envelopes):
        if idx < len(envelopes) - 1:
            gen.send(env)
        else:
            with pytest.raises(StopIteration) as exc:
                gen.send(env)
            return exc.value.value
    return None


def test_handle_await_all_returns_results_in_input_order():
    """Proves _handle_await_all returns results ordered by input position, not arrival order."""

    effects = [EAwait(fn_name="a"), EAwait(fn_name="b")]
    envelopes = [
        Envelope(sender=OVERSEER, body=(1, "result_b")),
        Envelope(sender=OVERSEER, body=(0, "result_a")),
    ]
    result = _drive_await_all(effects, envelopes)
    assert result == ["result_a", "result_b"]


def _drive_await_all_to_receive(effects):
    """Drive _handle_await_all past all EEnqueue yields, returning the generator at first EReceive."""
    gen = _handle_await_all(CTX, EAwaitAll(effects=effects))
    next(gen)
    for _ in effects:
        gen.send(None)
    return gen


def test_handle_await_all_raises_job_error_when_child_fails():
    """Proves _handle_await_all raises JobError when a child job returns an error."""

    effects = [EAwait(fn_name="a"), EAwait(fn_name="b")]
    error = JobError(ValueError("boom"))
    gen = _drive_await_all_to_receive(effects)
    gen.send(Envelope(sender=OVERSEER, body=(0, error)))
    with pytest.raises(JobError):
        gen.send(Envelope(sender=OVERSEER, body=(1, "ok")))


def test_handle_await_all_raises_job_timeout_when_child_times_out():
    """Proves _handle_await_all raises JobTimeout when a child job times out."""

    effects = [EAwait(fn_name="a"), EAwait(fn_name="b")]
    gen = _drive_await_all_to_receive(effects)
    gen.send(Envelope(sender=OVERSEER, body=(0, JobTimeout())))
    with pytest.raises(JobTimeout):
        gen.send(Envelope(sender=OVERSEER, body=(1, "ok")))


def test_handle_await_all_drains_all_replies_before_raising():
    """Proves _handle_await_all collects all N replies before raising the first error."""

    effects = [EAwait(fn_name="a"), EAwait(fn_name="b"), EAwait(fn_name="c")]
    error = JobError(ValueError("first"))
    gen = _drive_await_all_to_receive(effects)
    gen.send(Envelope(sender=OVERSEER, body=(0, error)))
    gen.send(Envelope(sender=OVERSEER, body=(1, "ok")))
    with pytest.raises(JobError):
        gen.send(Envelope(sender=OVERSEER, body=(2, "ok")))


def test_handle_await_all_raises_first_error_when_multiple_fail():
    """Proves _handle_await_all raises the first error encountered, not the last."""

    effects = [EAwait(fn_name="a"), EAwait(fn_name="b")]
    first_error = JobError(ValueError("first"))
    second_error = JobError(ValueError("second"))
    gen = _drive_await_all_to_receive(effects)
    gen.send(Envelope(sender=OVERSEER, body=(0, first_error)))
    with pytest.raises(JobError) as exc:
        gen.send(Envelope(sender=OVERSEER, body=(1, second_error)))
    assert exc.value is first_error


# make_handlers


def test_make_handlers_contains_all_effect_types():
    """Proves make_handlers returns entries for all handled effect types."""

    handlers = make_job_handlers(JobHandlerContext(overseer=OVERSEER))
    assert set(handlers.keys()) == {
        ESatisfied.tag,
        EImpossible.tag,
        EAwait.tag,
        EAwaitAll.tag,
        EAcquire.tag,
        ESignal.tag,
        ESetSemaphore.tag,
    }


def test_make_handlers_returns_callables():
    """Proves every handler value in make_handlers is callable."""

    handlers = make_job_handlers(JobHandlerContext(overseer=OVERSEER))
    assert all(callable(h) for h in handlers.values())
