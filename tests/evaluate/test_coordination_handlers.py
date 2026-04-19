from unittest.mock import patch

import pytest

from tertius import ESend, ESelf, ESleep, Pid

from zahir.core.constants import ENQUEUE, GET_JOB, JOB_DONE, RELEASE
from zahir.core.effects import EEnqueue, EGetJob, EJobComplete, EJobFail, ERelease
from zahir.core.evaluate.coordination_handlers import (
    CoordinationHandlerContext,
    _handle_enqueue,
    _handle_get_job,
    _handle_job_complete,
    _handle_job_fail,
    _handle_release,
    make_coordination_handlers,
)
from zahir.core.exceptions import JobError, JobTimeout
from tests.evaluate.mocks import ME, OVERSEER, mock_mcall, mock_mcast

CTX = CoordinationHandlerContext(overseer=OVERSEER)

REPLY_TO = bytes(ME)


def _drive(gen):
    """Collect all yielded effects from a generator, returning them as a list."""
    effects = []
    try:
        effect = next(gen)
        while True:
            effects.append(effect)
            effect = gen.send(None)
    except StopIteration:
        pass
    return effects


# _handle_enqueue


def test_handle_enqueue_first_yields_eself():
    """Proves _handle_enqueue first yields ESelf to obtain the worker's own pid."""

    with patch("zahir.core.evaluate.coordination_handlers.mcast", mock_mcast()):
        gen = _handle_enqueue(CTX, EEnqueue(fn_name="child", args=(), timeout_ms=None, nonce=None))
        assert isinstance(next(gen), ESelf)


def test_handle_enqueue_sends_correct_message_to_overseer():
    """Proves _handle_enqueue sends the correct ENQUEUE tuple to the overseer."""

    sent = []

    def _capturing(pid, body):
        sent.append((pid, body))
        return None
        yield

    with patch("zahir.core.evaluate.coordination_handlers.mcast", _capturing):
        gen = _handle_enqueue(CTX, EEnqueue(fn_name="child", args=(1,), timeout_ms=500, nonce=3))
        next(gen)                  # ESelf
        with pytest.raises(StopIteration):
            gen.send(ME)           # provide our pid, triggers mcast then handler finishes

    pid, body = sent[0]
    assert pid == OVERSEER
    assert body[0] == ENQUEUE
    assert body[1] == "child"
    assert body[2] == (1,)
    assert body[4] == 500
    assert body[5] == 3


# _handle_get_job


def test_handle_get_job_returns_job_when_available():
    """Proves _handle_get_job returns the job immediately when the overseer has one."""

    job = ("fn", (), None, None, None)
    with patch("zahir.core.evaluate.coordination_handlers.mcall", mock_mcall(job)):
        gen = _handle_get_job(CTX, EGetJob())
        with pytest.raises(StopIteration) as exc:
            next(gen)
        assert exc.value.value == job


def test_handle_get_job_sleeps_then_retries_when_none():
    """Proves _handle_get_job yields ESleep and retries when no job is available."""

    job = ("fn", (), None, None, None)
    responses = iter([None, job])

    def _mcall_sequence(pid, body):
        return next(responses)
        yield

    with patch("zahir.core.evaluate.coordination_handlers.mcall", _mcall_sequence):
        gen = _handle_get_job(CTX, EGetJob())
        effect = next(gen)
        assert isinstance(effect, ESleep)


# _handle_job_complete


def test_handle_job_complete_sends_result_to_reply_to():
    """Proves _handle_job_complete yields ESend with the result when reply_to is set."""

    with patch("zahir.core.evaluate.coordination_handlers.mcast", mock_mcast()):
        gen = _handle_job_complete(CTX, EJobComplete(result="done", reply_to=REPLY_TO, nonce=7))
        effect = next(gen)
        assert isinstance(effect, ESend)
        assert effect.body == (7, "done")


def test_handle_job_complete_skips_esend_when_no_reply_to():
    """Proves _handle_job_complete does not yield ESend when reply_to is None."""

    with patch("zahir.core.evaluate.coordination_handlers.mcast", mock_mcast()):
        effects = _drive(_handle_job_complete(CTX, EJobComplete(result="done", reply_to=None, nonce=None)))
        assert not any(isinstance(eff, ESend) for eff in effects)


def test_handle_job_complete_mcasts_job_done():
    """Proves _handle_job_complete always sends JOB_DONE to the overseer."""

    sent = []

    def _capturing(pid, body):
        sent.append((pid, body))
        return None
        yield

    with patch("zahir.core.evaluate.coordination_handlers.mcast", _capturing):
        effects = _drive(_handle_job_complete(CTX, EJobComplete(result="done", reply_to=None, nonce=None)))

    assert any(pid == OVERSEER and body == JOB_DONE for pid, body in sent)


# _handle_job_fail


def test_handle_job_fail_sends_error_to_reply_to():
    """Proves _handle_job_fail yields ESend with the error when reply_to is set."""

    err = JobError(ValueError("boom"))
    with patch("zahir.core.evaluate.coordination_handlers.mcast", mock_mcast()):
        gen = _handle_job_fail(CTX, EJobFail(error=err, reply_to=REPLY_TO, nonce=5))
        effect = next(gen)
        assert isinstance(effect, ESend)
        assert effect.body == (5, err)


def test_handle_job_fail_mcasts_job_done_with_reply_to():
    """Proves _handle_job_fail sends JOB_DONE to the overseer when reply_to is set."""

    sent = []

    def _capturing(pid, body):
        sent.append((pid, body))
        return None
        yield

    err = JobTimeout()
    with patch("zahir.core.evaluate.coordination_handlers.mcast", _capturing):
        effects = _drive(_handle_job_fail(CTX, EJobFail(error=err, reply_to=REPLY_TO, nonce=0)))

    assert any(pid == OVERSEER and body == JOB_DONE for pid, body in sent)


def test_handle_job_fail_mcasts_job_done_with_error_when_no_reply_to():
    """Proves _handle_job_fail sends (JOB_DONE, error) to the overseer when reply_to is None."""

    sent = []

    def _capturing(pid, body):
        sent.append((pid, body))
        return None
        yield

    err = JobError(ValueError("boom"))
    with patch("zahir.core.evaluate.coordination_handlers.mcast", _capturing):
        _drive(_handle_job_fail(CTX, EJobFail(error=err, reply_to=None, nonce=None)))

    assert any(pid == OVERSEER and body == (JOB_DONE, err) for pid, body in sent)


def test_handle_job_fail_skips_esend_when_no_reply_to():
    """Proves _handle_job_fail does not yield ESend when reply_to is None."""

    with patch("zahir.core.evaluate.coordination_handlers.mcast", mock_mcast()):
        effects = _drive(_handle_job_fail(CTX, EJobFail(error=JobTimeout(), reply_to=None, nonce=None)))
        assert not any(isinstance(eff, ESend) for eff in effects)


# _handle_release


def test_handle_release_mcasts_release_with_name():
    """Proves _handle_release sends (RELEASE, name) to the overseer."""

    sent = []

    def _capturing(pid, body):
        sent.append((pid, body))
        return None
        yield

    with patch("zahir.core.evaluate.coordination_handlers.mcast", _capturing):
        gen = _handle_release(CTX, ERelease(name="workers"))
        with pytest.raises(StopIteration):
            next(gen)

    assert sent[0] == (OVERSEER, (RELEASE, "workers"))


# make_coordination_handlers


def test_make_coordination_handlers_contains_all_effect_types():
    """Proves make_coordination_handlers returns entries for all coordination effect tags."""

    handlers = make_coordination_handlers(CTX)
    assert set(handlers.keys()) == {
        EEnqueue.tag,
        EGetJob.tag,
        EJobComplete.tag,
        EJobFail.tag,
        ERelease.tag,
    }


def test_make_coordination_handlers_returns_callables():
    """Proves every handler value in make_coordination_handlers is callable."""

    handlers = make_coordination_handlers(CTX)
    assert all(callable(hdl) for hdl in handlers.values())
