from unittest.mock import patch

import pytest
from bookman.events import Event
from tertius import EEmit

from tests.evaluate.mocks import ME, OVERSEER, mock_mcall, mock_mcall_timeout, mock_mcast
from tests.shared import drain_to
from zahir.core.combinators import wrap
from zahir.core.constants import WORKER_PARK_TIMEOUT_MS, WorkItemTag
from zahir.core.effects import (
    EEnqueue,
    EGetJob,
    EGetState,
    EJobComplete,
    EJobFail,
    ESetState,
    EStorageAcquire,
    EStorageEnqueue,
    EStorageGetError,
    EStorageGetJob,
    EStorageGetResult,
    EStorageGetState,
    EStorageIsDone,
    EStorageJobDone,
    EStorageJobFailed,
    EStorageRelease,
    EStorageSetState,
)
from zahir.core.evaluate.coordination_handlers import (
    _call_overseer,
    _cast_overseer,
    _handle_enqueue,
    _handle_get_job,
    _handle_get_state,
    _handle_job_complete,
    _handle_job_fail,
    _handle_set_state,
    make_coordination_handlers,
)
from zahir.core.exceptions import JobError, OverseerSilentError
from zahir.core.telemetry import make_telemetry
from zahir.core.zahir_types import JobSpec, LeaseTracker, SilenceTracker

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

    child_spec = JobSpec(
        fn_name="child", args=(1,), reply_to=WORKER_PID, timeout_ms=500, sequence_number=3
    )
    with patch("zahir.core.evaluate.coordination_handlers.mcast", _capturing):
        drain_to(_handle_enqueue(OVERSEER, EEnqueue(job=child_spec)))

    assert sent[0] == (OVERSEER, EStorageEnqueue(job=child_spec))


# _handle_get_job

MCALL_TIMEOUT_PATH = "zahir.core.evaluate.coordination_handlers.mcall_timeout"


def run_get_job(reply, silence: SilenceTracker, lease: LeaseTracker | None = None):
    """Drive _handle_get_job against a canned overseer reply; return its result."""

    with patch(MCALL_TIMEOUT_PATH, mock_mcall_timeout(reply)):
        gen = _handle_get_job(
            OVERSEER, silence, lease or LeaseTracker(), EGetJob(worker_pid_bytes=WORKER_PID)
        )
        with pytest.raises(StopIteration) as exc:
            next(gen)
        return exc.value.value


def test_handle_get_job_returns_work_from_overseer():
    """Proves _handle_get_job unwraps a leased work reply and returns the work item."""

    work = JobSpec(fn_name="fn")
    assert run_get_job((1, work), SilenceTracker()) == work


def test_handle_get_job_records_lease_id_for_next_ack():
    """Proves a received lease id is recorded so the next request acks it."""

    lease = LeaseTracker()
    work = JobSpec(fn_name="fn")
    run_get_job((9, work), SilenceTracker(), lease)
    assert lease.ack == 9


def test_handle_get_job_sends_recorded_ack_to_overseer():
    """Proves the next EStorageGetJob carries the last received lease id as its ack."""

    sent = []

    def _capturing(pid, body, timeout_ms):
        sent.append(body)
        return None
        yield

    lease = LeaseTracker(ack=9)
    get_job = EGetJob(worker_pid_bytes=WORKER_PID)
    with patch(MCALL_TIMEOUT_PATH, _capturing):
        gen = _handle_get_job(OVERSEER, SilenceTracker(), lease, get_job)
        with pytest.raises(StopIteration):
            next(gen)

    assert sent[0] == EStorageGetJob(worker_pid_bytes=WORKER_PID, ack=9)


def test_handle_get_job_maps_no_work_ack_to_none():
    """Proves a NO_WORK wake or heartbeat ack maps to None so the worker re-requests."""

    assert run_get_job(WorkItemTag.NO_WORK, SilenceTracker()) is None


def test_handle_get_job_returns_none_on_heartbeat_timeout():
    """Proves a reply-less park window maps to None so the worker re-requests."""

    assert run_get_job(None, SilenceTracker()) is None


def test_handle_get_job_raises_after_prolonged_silence():
    """Proves accumulated reply-less windows past max_silence_ms raise OverseerSilentError."""

    silence = SilenceTracker(max_silence_ms=2 * WORKER_PARK_TIMEOUT_MS)
    assert run_get_job(None, silence) is None
    with pytest.raises(OverseerSilentError):
        run_get_job(None, silence)


def test_handle_get_job_reply_resets_silence():
    """Proves any overseer reply resets the accumulated silence window."""

    silence = SilenceTracker(max_silence_ms=2 * WORKER_PARK_TIMEOUT_MS)
    assert run_get_job(None, silence) is None
    assert run_get_job(WorkItemTag.NO_WORK, silence) is None
    assert silence.silent_ms == 0


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


# _cast_overseer


def test_cast_overseer_mcasts_storage_release_with_name():
    """Proves _cast_overseer sends the storage effect to the overseer unchanged."""

    sent = []

    def _capturing(pid, body):
        sent.append((pid, body))
        return None
        yield

    with patch("zahir.core.evaluate.coordination_handlers.mcast", _capturing):
        gen = _cast_overseer(OVERSEER, EStorageRelease(name="workers"))
        with pytest.raises(StopIteration):
            next(gen)

    assert sent[0] == (OVERSEER, EStorageRelease(name="workers"))


# _call_overseer


def test_call_overseer_mcalls_storage_acquire():
    """Proves _call_overseer sends the storage effect to the overseer and returns the reply."""

    sent = []

    def _capturing(pid, body):
        sent.append((pid, body))
        return True
        yield

    with patch("zahir.core.evaluate.coordination_handlers.mcall", _capturing):
        gen = _call_overseer(OVERSEER, EStorageAcquire(name="workers", limit=4))
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
        EEnqueue.tag,
        EGetJob.tag,
        EJobComplete.tag,
        EJobFail.tag,
        EGetState.tag,
        ESetState.tag,
        EStorageAcquire.tag,
        EStorageRelease.tag,
        EStorageIsDone.tag,
        EStorageGetError.tag,
        EStorageGetResult.tag,
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
    """Proves the EStorageIsDone transport handler is wrapped when handler_wrappers is set."""

    emitted = []

    def recording_fn(effect):
        emitted.append(effect.tag)
        yield

    handlers = make_coordination_handlers(OVERSEER, [wrap(recording_fn)])

    with patch(MCALL_TIMEOUT_PATH, mock_mcall_timeout(False)):
        drain_to(handlers["storage_is_done"](EStorageIsDone()))

    assert EStorageIsDone.tag in emitted


def test_make_coordination_handlers_applies_wrapper_to_all_root_effects():
    """Proves handler_wrappers covers all three root polling effect types."""

    seen_tags = []

    def recording_fn(effect):
        seen_tags.append(effect.tag)
        yield

    handlers = make_coordination_handlers(OVERSEER, [wrap(recording_fn)])

    cases = [(EStorageIsDone, False), (EStorageGetError, None), (EStorageGetResult, None)]
    for effect_type, reply in cases:
        with (
            patch("zahir.core.evaluate.coordination_handlers.mcall", mock_mcall(reply)),
            patch(MCALL_TIMEOUT_PATH, mock_mcall_timeout(reply)),
        ):
            drain_to(handlers[effect_type.tag](effect_type()))

    for effect_type, _reply in cases:
        assert effect_type.tag in seen_tags


def test_make_coordination_handlers_emits_telemetry_events_for_is_done():
    """Proves bookman Events are emitted when make_telemetry is applied."""

    handlers = make_coordination_handlers(OVERSEER, [make_telemetry()])

    with patch(MCALL_TIMEOUT_PATH, mock_mcall_timeout(False)):
        emitted, _ = drain_to(handlers["storage_is_done"](EStorageIsDone()))

    telemetry = [e.body for e in emitted if isinstance(e, EEmit) and isinstance(e.body, Event)]
    assert len(telemetry) == 2  # start point + end span
