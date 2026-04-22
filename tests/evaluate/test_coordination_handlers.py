from unittest.mock import patch

import pytest

from tertius import EEmit, ESleep, Pid

from zahir.core.constants import (
    ACQUIRE,
    ENQUEUE,
    GET_JOB,
    JOB_DONE,
    JOB_FAILED,
    RELEASE,
    SET_SEMAPHORE,
    SIGNAL,
)
from zahir.core.effects import (
    EAcquireSlot,
    EEnqueue,
    EGetJob,
    EJobComplete,
    EJobFail,
    ERelease,
    ESetSemaphoreState,
    ESignal,
)
from zahir.core.evaluate.coordination_handlers import (
    CoordinationHandlerContext,
    _handle_acquire_slot,
    _handle_enqueue,
    _handle_get_job,
    _handle_job_complete,
    _handle_job_fail,
    _handle_release,
    _handle_set_semaphore_state,
    _handle_signal,
    make_coordination_handlers,
)
from zahir.core.exceptions import JobError, JobTimeout
from bookman.events import Event
from zahir.progress_bar.telemetry import make_telemetry
from tests.evaluate.mocks import ME, OVERSEER, mock_mcall, mock_mcast

CTX = CoordinationHandlerContext(overseer=OVERSEER)

REPLY_TO = bytes(ME)
WORKER_PID = b"worker-pid"


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


def test_handle_enqueue_sends_correct_message_to_overseer():
    """Proves _handle_enqueue sends the correct ENQUEUE tuple to the overseer."""

    sent = []

    def _capturing(pid, body):
        sent.append((pid, body))
        return None
        yield

    with patch("zahir.core.evaluate.coordination_handlers.mcast", _capturing):
        _drive(
            _handle_enqueue(
                CTX,
                EEnqueue(
                    fn_name="child",
                    args=(1,),
                    reply_to=WORKER_PID,
                    timeout_ms=500,
                    sequence_number=3,
                ),
            )
        )

    assert sent[0] == (OVERSEER, (ENQUEUE, "child", (1,), WORKER_PID, 500, 3))


# _handle_get_job


def test_handle_get_job_returns_work_from_overseer():
    """Proves _handle_get_job returns whatever the overseer replies with."""

    work = ("job", "fn", (), None, None, None)
    with patch("zahir.core.evaluate.coordination_handlers.mcall", mock_mcall(work)):
        gen = _handle_get_job(CTX, EGetJob(worker_pid_bytes=WORKER_PID))
        with pytest.raises(StopIteration) as exc:
            next(gen)
        assert exc.value.value == work


def test_handle_get_job_returns_none_when_overseer_has_nothing():
    """Proves _handle_get_job returns None without looping when the overseer has nothing."""

    with patch("zahir.core.evaluate.coordination_handlers.mcall", mock_mcall(None)):
        gen = _handle_get_job(CTX, EGetJob(worker_pid_bytes=WORKER_PID))
        with pytest.raises(StopIteration) as exc:
            next(gen)
        assert exc.value.value is None


# _handle_job_complete


def test_handle_job_complete_mcasts_job_done_with_result():
    """Proves _handle_job_complete sends (JOB_DONE, reply_to, sequence_number, result) to the overseer."""

    sent = []

    def _capturing(pid, body):
        sent.append((pid, body))
        return None
        yield

    with patch("zahir.core.evaluate.coordination_handlers.mcast", _capturing):
        _drive(
            _handle_job_complete(
                CTX, EJobComplete(result="done", reply_to=REPLY_TO, sequence_number=7)
            )
        )

    assert sent[0] == (OVERSEER, (JOB_DONE, REPLY_TO, 7, "done"))


def test_handle_job_complete_with_none_reply_to():
    """Proves _handle_job_complete sends (JOB_DONE, None, None, result) for the root job."""

    sent = []

    def _capturing(pid, body):
        sent.append((pid, body))
        return None
        yield

    with patch("zahir.core.evaluate.coordination_handlers.mcast", _capturing):
        _drive(
            _handle_job_complete(
                CTX, EJobComplete(result="done", reply_to=None, sequence_number=None)
            )
        )

    assert sent[0] == (OVERSEER, (JOB_DONE, None, None, "done"))


# _handle_job_fail


def test_handle_job_fail_routes_error_to_parent_via_overseer():
    """Proves _handle_job_fail sends (JOB_DONE, reply_to, sequence_number, error) when reply_to is set."""

    sent = []

    def _capturing(pid, body):
        sent.append((pid, body))
        return None
        yield

    err = JobError(ValueError("boom"))
    with patch("zahir.core.evaluate.coordination_handlers.mcast", _capturing):
        _drive(
            _handle_job_fail(
                CTX, EJobFail(error=err, reply_to=REPLY_TO, sequence_number=5)
            )
        )

    assert sent[0] == (OVERSEER, (JOB_DONE, REPLY_TO, 5, err))


def test_handle_job_fail_sends_job_failed_for_root_job():
    """Proves _handle_job_fail sends (JOB_FAILED, error) to the overseer when reply_to is None."""

    sent = []

    def _capturing(pid, body):
        sent.append((pid, body))
        return None
        yield

    err = JobError(ValueError("boom"))
    with patch("zahir.core.evaluate.coordination_handlers.mcast", _capturing):
        _drive(
            _handle_job_fail(
                CTX, EJobFail(error=err, reply_to=None, sequence_number=None)
            )
        )

    assert sent[0] == (OVERSEER, (JOB_FAILED, err))


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


# _handle_acquire_slot


def test_handle_acquire_slot_mcalls_acquire():
    """Proves _handle_acquire_slot sends (ACQUIRE, name, limit) to the overseer and returns the result."""

    sent = []

    def _capturing(pid, body):
        sent.append((pid, body))
        return True
        yield

    with patch("zahir.core.evaluate.coordination_handlers.mcall", _capturing):
        gen = _handle_acquire_slot(CTX, EAcquireSlot(name="workers", limit=4))
        with pytest.raises(StopIteration) as exc:
            next(gen)

    assert sent[0] == (OVERSEER, (ACQUIRE, "workers", 4))
    assert exc.value.value is True


# _handle_signal


def test_handle_signal_mcalls_signal():
    """Proves _handle_signal sends (SIGNAL, name) to the overseer and returns the state."""

    sent = []

    def _capturing(pid, body):
        sent.append((pid, body))
        return "satisfied"
        yield

    with patch("zahir.core.evaluate.coordination_handlers.mcall", _capturing):
        gen = _handle_signal(CTX, ESignal(name="db"))
        with pytest.raises(StopIteration) as exc:
            next(gen)

    assert sent[0] == (OVERSEER, (SIGNAL, "db"))
    assert exc.value.value == "satisfied"


# _handle_set_semaphore_state


def test_handle_set_semaphore_state_mcasts_set_semaphore():
    """Proves _handle_set_semaphore_state sends (SET_SEMAPHORE, name, state) to the overseer."""

    sent = []

    def _capturing(pid, body):
        sent.append((pid, body))
        return None
        yield

    with patch("zahir.core.evaluate.coordination_handlers.mcast", _capturing):
        _drive(
            _handle_set_semaphore_state(
                CTX, ESetSemaphoreState(name="db", state="impossible")
            )
        )

    assert sent[0] == (OVERSEER, (SET_SEMAPHORE, "db", "impossible"))


# make_coordination_handlers


def test_make_coordination_handlers_contains_all_effect_types():
    """Proves make_coordination_handlers returns entries for all coordination effect tags."""

    handlers = make_coordination_handlers(CTX)
    assert set(handlers.keys()) == {
        EAcquireSlot.tag,
        EEnqueue.tag,
        EGetJob.tag,
        EJobComplete.tag,
        EJobFail.tag,
        ERelease.tag,
        ESetSemaphoreState.tag,
        ESignal.tag,
    }


def test_make_coordination_handlers_returns_callables():
    """Proves every handler value in make_coordination_handlers is callable."""

    handlers = make_coordination_handlers(CTX)
    assert all(callable(hdl) for hdl in handlers.values())


# handler_wrappers


def _collect(gen):
    """Drive a generator to completion, collecting yielded values."""
    values = []
    try:
        value = next(gen)
        while True:
            values.append(value)
            value = gen.send(None)
    except StopIteration:
        pass
    return values


def test_job_complete_handler_emits_telemetry_with_fn_name():
    """Proves EJobComplete handler emits telemetry events carrying fn_name when make_telemetry is applied."""

    ctx = CoordinationHandlerContext(
        overseer=OVERSEER, handler_wrappers=[make_telemetry()]
    )
    handlers = make_coordination_handlers(ctx)

    effect = EJobComplete(
        result="done", reply_to=REPLY_TO, sequence_number=7, fn_name="chapter_processor"
    )

    with patch("zahir.core.evaluate.coordination_handlers.mcast", mock_mcast()):
        emitted = _collect(handlers[EJobComplete.tag](effect))

    telemetry = [
        e.body for e in emitted if isinstance(e, EEmit) and isinstance(e.body, Event)
    ]
    assert any(e.dim("fn") == "chapter_processor" for e in telemetry)


def test_job_fail_handler_emits_telemetry_with_fn_name():
    """Proves EJobFail handler emits telemetry events carrying fn_name when make_telemetry is applied."""

    ctx = CoordinationHandlerContext(
        overseer=OVERSEER, handler_wrappers=[make_telemetry()]
    )
    handlers = make_coordination_handlers(ctx)

    effect = EJobFail(
        error=JobError(ValueError("boom")),
        reply_to=REPLY_TO,
        sequence_number=5,
        fn_name="chapter_processor",
    )

    with patch("zahir.core.evaluate.coordination_handlers.mcast", mock_mcast()):
        emitted = _collect(handlers[EJobFail.tag](effect))

    telemetry = [
        e.body for e in emitted if isinstance(e, EEmit) and isinstance(e.body, Event)
    ]
    assert any(e.dim("fn") == "chapter_processor" for e in telemetry)
