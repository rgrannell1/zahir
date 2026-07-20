"""Our overseer genserver delegates all work to storage handlers & further downstreams.

Idle workers and the completion waiter long-poll rather than spin: a call with nothing
to hand back parks the caller (NoReply) and is woken ack-only when work or completion
arrives. Acks never carry payloads, and payload-bearing work replies are leased by the
backend until the worker's next request acks receipt — so a reply dropped by a dead or
timed-out caller loses nothing; the work is re-delivered on the next heartbeat request.
"""

from collections.abc import Generator
from dataclasses import dataclass, field
from functools import partial
from typing import Any

from orbis import handle, tap
from tertius import Caller, EEmit, ERegister, ESend, NoReply, ReplyMsg, gen_server, reply_to

from zahir.core.constants import OVERSEER_NAME, ParkTag, WorkItemTag
from zahir.core.effects import (
    EStorageEnqueue,
    EStorageGetJob,
    EStorageIsDone,
    EStorageJobDone,
    EStorageJobFailed,
)
from zahir.core.emit import park_event


@dataclass
class ParkingLot:
    """Callers parked on long-poll requests, keyed by requester pid bytes."""

    workers: dict[bytes, Caller] = field(default_factory=dict)
    completion: dict[bytes, Caller] = field(default_factory=dict)


def _init() -> Generator[Any, Any, ParkingLot]:
    """Register the overseer's broker name so remote workers can find it.

    The root job is enqueued by _root after all workers are spawned.
    """

    yield ERegister(name=OVERSEER_NAME)
    return ParkingLot()


def park_or_ack(parked: dict[bytes, Caller], key: bytes, caller: Caller, ack: Any) -> Any:
    """Park a first request as NoReply; ack a heartbeat retry so the caller re-requests."""

    if key in parked:
        del parked[key]
        return ack
    parked[key] = caller
    return NoReply


def get_job_reply(
    lot: ParkingLot, effect: EStorageGetJob, caller: Caller
) -> Generator[Any, Any, Any]:
    """Hand back work, or park the idle worker until some arrives."""

    work = yield effect
    if work is not None:
        # A stale heartbeat entry would waste a later wake on a busy worker.
        lot.workers.pop(effect.worker_pid_bytes, None)
        return work
    return park_or_ack(lot.workers, effect.worker_pid_bytes, caller, WorkItemTag.NO_WORK)


def is_done_reply(
    lot: ParkingLot, effect: EStorageIsDone, caller: Caller
) -> Generator[Any, Any, Any]:
    """Reply True when the workflow is finished, or park the waiter until it is."""

    done = yield effect
    key = bytes(caller.sender)
    if done:
        lot.completion.pop(key, None)
        return True
    return park_or_ack(lot.completion, key, caller, False)


def _handle_call(
    lot: ParkingLot, body: Any, caller: Caller
) -> Generator[Any, Any, tuple[ParkingLot, Any]]:
    """Pass the storage effect through to the handle() layer, parking empty-handed callers."""

    match body:
        case EStorageGetJob():
            reply = yield from get_job_reply(lot, body, caller)
        case EStorageIsDone():
            reply = yield from is_done_reply(lot, body, caller)
        case _:
            reply = yield body
    return lot, reply


def wake_worker(lot: ParkingLot, worker_pid_bytes: bytes) -> Generator[Any, Any, None]:
    """Ack a specific parked worker so it re-requests and collects its work."""

    caller = lot.workers.pop(worker_pid_bytes, None)
    if caller is not None:
        yield from reply_to(caller, WorkItemTag.NO_WORK)


def wake_any_worker(lot: ParkingLot) -> Generator[Any, Any, None]:
    """Ack one parked worker, if any — a new job can go to whoever asks first."""

    if lot.workers:
        yield from wake_worker(lot, next(iter(lot.workers)))


def wake_completion_waiters(lot: ParkingLot, done: bool) -> Generator[Any, Any, None]:
    """Reply True to every parked completion waiter once the workflow is finished."""

    yield from ()
    if not done or not lot.completion:
        return
    for caller in lot.completion.values():
        yield from reply_to(caller, True)
    lot.completion.clear()


def _handle_cast(lot: ParkingLot, body: Any) -> Generator[Any, Any, ParkingLot]:
    """Pass the storage effect through to the handle() layer, then wake parked callers.

    The job-done/job-failed handlers return the workflow's done flag, so waking
    completion waiters costs no second storage dispatch."""

    handled = yield body
    match body:
        case EStorageEnqueue():
            yield from wake_any_worker(lot)
        case EStorageJobDone(reply_to=parent_pid_bytes):
            if parent_pid_bytes is not None:
                yield from wake_worker(lot, parent_pid_bytes)
            yield from wake_completion_waiters(lot, bool(handled))
        case EStorageJobFailed():
            yield from wake_completion_waiters(lot, bool(handled))
    return lot


def park_kind(body: Any) -> str:
    """Name the parked-caller kind for telemetry."""

    return "completion" if isinstance(body, EStorageIsDone) else "worker"


def wake_kind(reply_body: Any) -> str:
    """Name the woken-caller kind for telemetry — completion wakes carry True."""

    return "completion" if reply_body is True else "worker"


def emitting_parks(handler, lot: ParkingLot, body: Any, caller: Caller):
    """Telemetry decoration of handle_call — identical logic, plus a parked event."""

    state, reply = yield from handler(lot, body, caller)
    if reply is NoReply:
        yield EEmit(park_event(ParkTag.PARKED, park_kind(body), bytes(caller.sender).hex()))
    return state, reply


def emit_wake_event(effect: ESend) -> Generator[Any, Any, None]:
    """Emit a woken telemetry point for each reply the cast handler sends."""

    if isinstance(effect.body, ReplyMsg):
        kind = wake_kind(effect.body.body)
        yield EEmit(park_event(ParkTag.WOKEN, kind, bytes(effect.pid).hex()))


def emitting_wakes(handler, lot: ParkingLot, body: Any):
    """Telemetry decoration of handle_cast — identical logic, plus a woken event per reply."""

    return (yield from tap(handler(lot, body), ESend.tag, emit_wake_event))


def run_overseer(
    handlers: dict,
) -> Generator[Any, Any, None]:
    """Run the overseer genserver. Receives the full handler bag; dispatches its storage slice."""

    overseer = gen_server(
        init=_init,
        handle_call=partial(emitting_parks, _handle_call),
        handle_cast=partial(emitting_wakes, _handle_cast),
    )

    yield from handle(overseer(), handlers)
