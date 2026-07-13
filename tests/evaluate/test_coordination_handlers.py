from unittest.mock import patch

import pytest
from bookman.events import Event
from tertius import EEmit

from tests.evaluate.mocks import ME, OVERSEER, mock_mcall, mock_mcast
from tests.shared import drain_to
from zahir.core.combinators import wrap
from zahir.core.effects import (
    EAcquireSlot,
    EEnqueue,
    EGetError,
    EGetJob,
    EGetResult,
    EGetState,
    EIsDone,
    EJobComplete,
    EJobFail,
    ERelease,
    ESetState,
    EStorageAcquire,
    EStorageEnqueue,
    EStorageGetState,
    EStorageJobDone,
    EStorageJobFailed,
    EStorageRelease,
    EStorageSetState,
)
from zahir.core.evaluate.coordination_handlers import (
    _handle_acquire_slot,
    _handle_enqueue,
    _handle_get_job,
    _handle_get_state,
    _handle_job_complete,
    _handle_job_fail,
    _handle_release,
    _handle_set_state,
    make_coordination_handlers,
)
from zahir.core.exceptions import JobError
from zahir.core.telemetry import make_telemetry

REPLY_TO = bytes(ME)
WORKER_PID = b"worker-pid"


# _handle_enqueue


def test_handle_enqueue_sends_correct_message_to_overseer():
    """Proves _handle_enqueue sends the correct EStorageEnqueue to the overseer."""

    sent = []

    def _capturing(pid, body):
        sent.append((pid, body))
        return None
        yield

    with patch("zahir.core.evaluate.coordination_handlers.mcast", _capturing):
        drain_to(
            _handle_enqueue(
                OVERSEER,
                EEnqueue(
                    fn_name="child",
                    args=(1,),
                    reply_to=WORKER_PID,
                    timeout_ms=500,
                    sequence_number=3,
                ),
            )
        )

    storage_enqueue = EStorageEnqueue(
        fn_name="child",
        args=(1,),
        reply_to=WORKER_PID,
        timeout_ms=500,
        sequence_number=3,
    )
    assert sent[0] == (OVERSEER, storage_enqueue)


# _handle_get_job


def test_handle_get_job_returns_work_from_overseer():
    """Proves _handle_get_job returns whatever the overseer replies with."""

    work = ("job", "fn", (), None, None, None)
    with patch("zahir.core.evaluate.coordination_handlers.mcall", mock_mcall(work)):
        gen = _handle_get_job(OVERSEER, EGetJob(worker_pid_bytes=WORKER_PID))
        with pytest.raises(StopIteration) as exc:
            next(gen)
        assert exc.value.value == work


def test_handle_get_job_returns_none_when_overseer_has_nothing():
    """Proves _handle_get_job returns None without looping when the overseer has nothing."""

    with patch("zahir.core.evaluate.coordination_handlers.mcall", mock_mcall(None)):
        gen = _handle_get_job(OVERSEER, EGetJob(worker_pid_bytes=WORKER_PID))
        with pytest.raises(StopIteration) as exc:
            next(gen)
        assert exc.value.value is None


# _handle_job_complete


def test_handle_job_complete_mcasts_storage_job_done_with_result():
    """Proves _handle_job_complete sends EStorageJobDone with the result to the overseer."""

    sent = []

    def _capturing(pid, body):
        sent.append((pid, body))
        return None
        yield

    job_complete_effect = EJobComplete(
        result="done", reply_to=REPLY_TO, sequence_number=7
    )
    with patch("zahir.core.evaluate.coordination_handlers.mcast", _capturing):
        drain_to(_handle_job_complete(OVERSEER, job_complete_effect))

    assert sent[0] == (OVERSEER, EStorageJobDone(reply_to=REPLY_TO, sequence_number=7, body="done"))


def test_handle_job_complete_with_none_reply_to():
    """Proves _handle_job_complete sends EStorageJobDone with None reply_to for the root job."""

    sent = []

    def _capturing(pid, body):
        sent.append((pid, body))
        return None
        yield

    job_complete_root = EJobComplete(
        result="done", reply_to=None, sequence_number=None
    )
    with patch("zahir.core.evaluate.coordination_handlers.mcast", _capturing):
        drain_to(_handle_job_complete(OVERSEER, job_complete_root))

    assert sent[0] == (OVERSEER, EStorageJobDone(reply_to=None, sequence_number=None, body="done"))


# _handle_job_fail


def test_handle_job_fail_routes_error_to_parent_via_overseer():
    """Proves _handle_job_fail sends EStorageJobDone with the error when reply_to is set."""

    sent = []

    def _capturing(pid, body):
        sent.append((pid, body))
        return None
        yield

    err = JobError(ValueError("boom"))
    with patch("zahir.core.evaluate.coordination_handlers.mcast", _capturing):
        drain_to(
            _handle_job_fail(OVERSEER, EJobFail(error=err, reply_to=REPLY_TO, sequence_number=5))
        )

    assert sent[0] == (OVERSEER, EStorageJobDone(reply_to=REPLY_TO, sequence_number=5, body=err))


def test_handle_job_fail_sends_storage_job_failed_for_root_job():
    """Proves _handle_job_fail sends EStorageJobFailed to the overseer when reply_to is None."""

    sent = []

    def _capturing(pid, body):
        sent.append((pid, body))
        return None
        yield

    err = JobError(ValueError("boom"))
    with patch("zahir.core.evaluate.coordination_handlers.mcast", _capturing):
        drain_to(
            _handle_job_fail(OVERSEER, EJobFail(error=err, reply_to=None, sequence_number=None))
        )

    assert sent[0] == (OVERSEER, EStorageJobFailed(error=err))


# _handle_release


def test_handle_release_mcasts_storage_release_with_name():
    """Proves _handle_release sends EStorageRelease to the overseer."""

    sent = []

    def _capturing(pid, body):
        sent.append((pid, body))
        return None
        yield

    with patch("zahir.core.evaluate.coordination_handlers.mcast", _capturing):
        gen = _handle_release(OVERSEER, ERelease(name="workers"))
        with pytest.raises(StopIteration):
            next(gen)

    assert sent[0] == (OVERSEER, EStorageRelease(name="workers"))


# _handle_acquire_slot


def test_handle_acquire_slot_mcalls_storage_acquire():
    """Proves _handle_acquire_slot sends EStorageAcquire to the overseer and returns the result."""

    sent = []

    def _capturing(pid, body):
        sent.append((pid, body))
        return True
        yield

    with patch("zahir.core.evaluate.coordination_handlers.mcall", _capturing):
        gen = _handle_acquire_slot(OVERSEER, EAcquireSlot(name="workers", limit=4))
        with pytest.raises(StopIteration) as exc:
            next(gen)

    assert sent[0] == (OVERSEER, EStorageAcquire(name="workers", limit=4))
    assert exc.value.value is True


# _handle_get_state


def test_handle_get_state_mcalls_storage_get_state():
    """Proves _handle_get_state sends EStorageGetState to the overseer and returns the value."""

    sent = []

    def _capturing(pid, body):
        sent.append((pid, body))
        return "satisfied"
        yield

    with patch("zahir.core.evaluate.coordination_handlers.mcall", _capturing):
        gen = _handle_get_state(OVERSEER, EGetState(name="db"))
        with pytest.raises(StopIteration) as exc:
            next(gen)

    assert sent[0] == (OVERSEER, EStorageGetState(name="db"))
    assert exc.value.value == "satisfied"


# _handle_set_state


def test_handle_set_state_mcasts_storage_set_state():
    """Proves _handle_set_state sends EStorageSetState to the overseer."""

    sent = []

    def _capturing(pid, body):
        sent.append((pid, body))
        return None
        yield

    with patch("zahir.core.evaluate.coordination_handlers.mcast", _capturing):
        drain_to(_handle_set_state(OVERSEER, ESetState(name="db", value="impossible")))

    assert sent[0] == (OVERSEER, EStorageSetState(name="db", state="impossible"))


# make_coordination_handlers


def test_make_coordination_handlers_contains_all_effect_types():
    """Proves handlers covers all coordination and root polling effect tags."""

    handlers = make_coordination_handlers(OVERSEER, [])
    assert set(handlers.keys()) == {
        EAcquireSlot.tag,
        EEnqueue.tag,
        EGetError.tag,
        EGetJob.tag,
        EGetResult.tag,
        EIsDone.tag,
        EJobComplete.tag,
        EJobFail.tag,
        ERelease.tag,
        EGetState.tag,
        ESetState.tag,
    }


def test_make_coordination_handlers_returns_callables():
    """Proves every handler value in make_coordination_handlers is callable."""

    handlers = make_coordination_handlers(OVERSEER, [])
    assert all(callable(hdl) for hdl in handlers.values())


# handler_wrappers


def test_job_complete_handler_emits_telemetry_with_fn_name():
    """Proves EJobComplete handler emits telemetry events carrying fn_name."""

    handlers = make_coordination_handlers(OVERSEER, [make_telemetry()])

    effect = EJobComplete(
        result="done",
        reply_to=REPLY_TO,
        sequence_number=7,
        fn_name="chapter_processor",
    )

    with patch("zahir.core.evaluate.coordination_handlers.mcast", mock_mcast()):
        emitted, _ = drain_to(handlers["job_complete"](effect))

    telemetry = [e.body for e in emitted if isinstance(e, EEmit) and isinstance(e.body, Event)]
    assert any(e.dim("fn") == "chapter_processor" for e in telemetry)


def test_job_fail_handler_emits_telemetry_with_fn_name():
    """Proves EJobFail handler emits telemetry events carrying fn_name."""

    handlers = make_coordination_handlers(OVERSEER, [make_telemetry()])

    effect = EJobFail(
        error=JobError(ValueError("boom")),
        reply_to=REPLY_TO,
        sequence_number=5,
        fn_name="chapter_processor",
    )

    with patch("zahir.core.evaluate.coordination_handlers.mcast", mock_mcast()):
        emitted, _ = drain_to(handlers["job_fail"](effect))

    telemetry = [e.body for e in emitted if isinstance(e, EEmit) and isinstance(e.body, Event)]
    assert any(e.dim("fn") == "chapter_processor" for e in telemetry)


# make_coordination_handlers — root polling effects


def test_make_coordination_handlers_applies_wrapper_to_is_done():
    """Proves the EIsDone handler is wrapped when handler_wrappers is set."""

    emitted = []

    def recording_fn(effect):
        emitted.append(effect.tag)
        yield

    handlers = make_coordination_handlers(OVERSEER, [wrap(recording_fn)])

    with patch("zahir.core.evaluate.coordination_handlers.mcall", mock_mcall(False)):
        drain_to(handlers["is_done"](EIsDone()))

    assert EIsDone.tag in emitted


def test_make_coordination_handlers_applies_wrapper_to_all_root_effects():
    """Proves handler_wrappers covers all three root polling effect types."""

    seen_tags = []

    def recording_fn(effect):
        seen_tags.append(effect.tag)
        yield

    handlers = make_coordination_handlers(OVERSEER, [wrap(recording_fn)])

    cases = [(EIsDone, False), (EGetError, None), (EGetResult, None)]
    for effect_type, reply in cases:
        with patch("zahir.core.evaluate.coordination_handlers.mcall", mock_mcall(reply)):
            drain_to(handlers[effect_type.tag](effect_type()))

    for effect_type, _reply in cases:
        assert effect_type.tag in seen_tags


def test_make_coordination_handlers_emits_telemetry_events_for_is_done():
    """Proves bookman Events are emitted when make_telemetry is applied."""

    handlers = make_coordination_handlers(OVERSEER, [make_telemetry()])

    with patch("zahir.core.evaluate.coordination_handlers.mcall", mock_mcall(False)):
        emitted, _ = drain_to(handlers["is_done"](EIsDone()))

    telemetry = [e.body for e in emitted if isinstance(e, EEmit) and isinstance(e.body, Event)]
    assert len(telemetry) == 2  # start point + end span
