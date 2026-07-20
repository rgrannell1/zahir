from collections.abc import Generator, Sequence
from functools import partial
from typing import Any

from tertius import Pid, mcall, mcall_timeout, mcast

from zahir.core.combinators import build_handler_map
from zahir.core.commons.constants import (
    COMPLETION_PARK_TIMEOUT_MS,
    WORKER_PARK_TIMEOUT_MS,
    WorkItemTag,
)
from zahir.core.commons.zahir_types import HandlerMap, LeaseTracker, SilenceTracker
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
    ZahirStorageEffect,
)
from zahir.core.exceptions import OverseerSilentError


def _handle_enqueue(overseer: Pid, effect: EEnqueue) -> Generator[Any, Any, None]:
    """Enqueue a child job with the overseer, routing the reply back to this worker."""

    yield from mcast(overseer, EStorageEnqueue(job=effect.job))


def record_silence(silence: SilenceTracker, window_ms: int) -> None:
    """Accumulate a reply-less park window; raise once the overseer has been silent too long."""

    silence.silent_ms += window_ms
    limit = silence.max_silence_ms
    if limit is not None and silence.silent_ms >= limit:
        raise OverseerSilentError(f"overseer sent no replies for {silence.silent_ms}ms")


def _handle_get_job(
    overseer: Pid, silence: SilenceTracker, lease: LeaseTracker, effect: EGetJob
) -> Generator[Any, Any, Any]:
    """Ask the overseer for work — returns a new job, a buffered result, or None.

    The overseer parks this call while it has nothing to hand back; the worker
    heartbeats after WORKER_PARK_TIMEOUT_MS. A NO_WORK ack (wake or heartbeat
    answer) maps to None so the worker simply asks again. A live overseer acks
    every heartbeat, so accumulated silence means it is gone.

    Work replies arrive leased as (lease_id, work): the request acks the last
    received lease so the overseer knows delivery succeeded, and a reply lost to
    a heartbeat timeout is re-delivered on the next request rather than lost.
    """

    get_job = EStorageGetJob(effect.worker_pid_bytes, ack=lease.ack)
    reply = yield from mcall_timeout(overseer, get_job, timeout_ms=WORKER_PARK_TIMEOUT_MS)
    if reply is None:
        record_silence(silence, WORKER_PARK_TIMEOUT_MS)
        return None

    silence.silent_ms = 0
    if reply == WorkItemTag.NO_WORK:
        return None

    lease_id, work = reply
    lease.ack = lease_id
    return work


def _handle_job_complete(overseer: Pid, effect: EJobComplete) -> Generator[Any, Any, None]:
    """Route the result to the parent worker via the overseer and decrement pending."""

    yield from mcast(
        overseer,
        EStorageJobDone(
            reply_to=effect.reply_to,
            sequence_number=effect.sequence_number,
            body=effect.result,
        ),
    )


def _handle_job_fail(overseer: Pid, effect: EJobFail) -> Generator[Any, Any, None]:
    """Route the failure to the parent worker via the overseer, or record it as root error."""

    if effect.reply_to is None:
        yield from mcast(overseer, EStorageJobFailed(error=effect.error))
        return

    yield from mcast(
        overseer,
        EStorageJobDone(
            reply_to=effect.reply_to,
            sequence_number=effect.sequence_number,
            body=effect.error,
        ),
    )


def _call_overseer(overseer: Pid, effect: ZahirStorageEffect) -> Generator[Any, Any, Any]:
    """Transport a storage effect to the overseer and return its reply."""

    return (yield from mcall(overseer, effect))


def _cast_overseer(overseer: Pid, effect: ZahirStorageEffect) -> Generator[Any, Any, None]:
    """Transport a fire-and-forget storage effect to the overseer."""

    yield from mcast(overseer, effect)


def _handle_get_state(overseer: Pid, effect: EGetState) -> Generator[Any, Any, str]:
    """Read a value from the overseer's key-value store by name."""

    return (yield from mcall(overseer, EStorageGetState(name=effect.name)))


def _handle_set_state(overseer: Pid, effect: ESetState) -> Generator[Any, Any, None]:
    """Write a value to the overseer's key-value store by name."""

    yield from mcast(overseer, EStorageSetState(name=effect.name, state=effect.value))


def _handle_is_done(overseer: Pid, effect: EStorageIsDone) -> Generator[Any, Any, bool]:
    """Ask the overseer whether all pending jobs have completed.

    The overseer parks this call until completion; a heartbeat timeout or a
    not-done ack both map to False so the runner simply asks again.
    """

    done = yield from mcall_timeout(overseer, effect, timeout_ms=COMPLETION_PARK_TIMEOUT_MS)
    return bool(done)


# Now, assemble all of the handlers and wrap them with telemetry context.
# EGetJob is wrapped like everything else: it long-polls at the overseer, so its
# spans measure per-worker idle time rather than emitting per-tick noise.
def make_coordination_handlers(
    overseer: Pid,
    handler_wrappers: Sequence,
    max_silence_ms: int | None = None,
) -> HandlerMap:
    """Create handlers for all coordination and transported storage effects.

    Handles job lifecycle (worker) and completion long-polling (root). Callers merge
    these last: transported storage tags must beat any locally-present storage
    handlers, which would otherwise run against a private backend. max_silence_ms
    bounds how long the overseer may answer no get-job heartbeat before the worker
    raises.
    """

    silence = SilenceTracker(max_silence_ms=max_silence_ms)
    lease = LeaseTracker()
    bindings = {
        EEnqueue.tag: partial(_handle_enqueue, overseer),
        EGetJob.tag: partial(_handle_get_job, overseer, silence, lease),
        EJobComplete.tag: partial(_handle_job_complete, overseer),
        EJobFail.tag: partial(_handle_job_fail, overseer),
        EGetState.tag: partial(_handle_get_state, overseer),
        ESetState.tag: partial(_handle_set_state, overseer),
        # Storage effects yielded worker- or runner-side are transported to the
        # overseer; the overseer's own bag binds these tags to the backend instead.
        EStorageAcquire.tag: partial(_call_overseer, overseer),
        EStorageRelease.tag: partial(_cast_overseer, overseer),
        EStorageIsDone.tag: partial(_handle_is_done, overseer),
        EStorageGetError.tag: partial(_call_overseer, overseer),
        EStorageGetResult.tag: partial(_call_overseer, overseer),
    }

    return build_handler_map(bindings, handler_wrappers)
