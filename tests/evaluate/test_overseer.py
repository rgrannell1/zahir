from orbis import complete, handle
from tertius import Caller, EEmit, ESend, NoReply, Pid, ReplyMsg

from tests.shared import drain_to
from zahir.core.backends.memory import make_memory_storage_handlers
from zahir.core.commons.constants import ParkTag, WorkItemTag
from zahir.core.commons.zahir_types import JobSpec
from zahir.core.effects import (
    EStorageAcquire,
    EStorageEnqueue,
    EStorageGetJob,
    EStorageIsDone,
    EStorageJobDone,
    EStorageRelease,
)
from zahir.core.evaluate.overseer import (
    ParkingLot,
    _handle_call,
    _handle_cast,
    _init,
    emitting_parks,
    emitting_wakes,
)

WORKER_PID = Pid(node_id=0, id=1)
WAITER_PID = Pid(node_id=0, id=2)
WORKER_CALLER = Caller(sender=WORKER_PID, ref=10)
WAITER_CALLER = Caller(sender=WAITER_PID, ref=20)


def make_lot() -> ParkingLot:
    return ParkingLot()


def sent_replies(effects: list) -> list[ReplyMsg]:
    return [effect.body for effect in effects if isinstance(effect, ESend)]


# _init


def test_init_returns_empty_parking_lot():
    """Proves _init registers the overseer name and starts with nothing parked."""

    _, lot = drain_to(_init())
    assert lot == ParkingLot()


# _handle_call pass-through


def test_handle_call_yields_body_effect():
    """Proves _handle_call yields the body unchanged for handle() to intercept."""

    storage_effect = EStorageAcquire("workers", 4)
    gen = _handle_call(make_lot(), storage_effect, WORKER_CALLER)
    yielded = next(gen)
    assert yielded is storage_effect


def test_handle_call_returns_state_and_handler_result():
    """Proves _handle_call returns (state, result) using the value sent back by handle()."""

    lot = make_lot()
    gen = _handle_call(lot, EStorageAcquire("workers", 4), WORKER_CALLER)
    _, return_value = drain_to(gen, responses={EStorageAcquire: True})
    assert return_value == (lot, True)


# _handle_call parking


def test_get_job_parks_worker_when_queue_is_empty():
    """Proves an empty-handed get-job call parks the worker instead of replying."""

    lot = make_lot()
    gen = _handle_call(lot, EStorageGetJob(bytes(WORKER_PID)), WORKER_CALLER)
    _, (_, reply) = drain_to(gen)

    assert reply is NoReply
    assert lot.workers == {bytes(WORKER_PID): WORKER_CALLER}


def test_get_job_heartbeat_retry_gets_no_work_ack():
    """Proves a repeat get-job call from a parked worker is acked so it re-requests."""

    lot = make_lot()
    lot.workers[bytes(WORKER_PID)] = Caller(sender=WORKER_PID, ref=9)

    gen = _handle_call(lot, EStorageGetJob(bytes(WORKER_PID)), WORKER_CALLER)
    _, (_, reply) = drain_to(gen)

    assert reply == WorkItemTag.NO_WORK
    assert lot.workers == {}


def test_get_job_with_work_drops_stale_parked_entry():
    """Proves handing work to a worker clears any stale heartbeat entry for it."""

    lot = make_lot()
    lot.workers[bytes(WORKER_PID)] = Caller(sender=WORKER_PID, ref=9)
    work = JobSpec(fn_name="fn")

    gen = _handle_call(lot, EStorageGetJob(bytes(WORKER_PID)), WORKER_CALLER)
    _, (_, reply) = drain_to(gen, responses={EStorageGetJob: work})

    assert reply == work
    assert lot.workers == {}


def test_is_done_parks_waiter_until_completion():
    """Proves an is-done call parks the waiter while jobs are still pending."""

    lot = make_lot()
    gen = _handle_call(lot, EStorageIsDone(), WAITER_CALLER)
    _, (_, reply) = drain_to(gen, responses={EStorageIsDone: False})

    assert reply is NoReply
    assert lot.completion == {bytes(WAITER_PID): WAITER_CALLER}


def test_is_done_replies_true_when_finished():
    """Proves an is-done call replies immediately once the workflow has completed."""

    lot = make_lot()
    gen = _handle_call(lot, EStorageIsDone(), WAITER_CALLER)
    _, (_, reply) = drain_to(gen, responses={EStorageIsDone: True})

    assert reply is True
    assert lot.completion == {}


# _handle_cast wakes


def test_handle_cast_yields_body_effect():
    """Proves _handle_cast yields the body unchanged for handle() to intercept."""

    storage_effect = EStorageRelease("workers")
    gen = _handle_cast(make_lot(), storage_effect)
    yielded = next(gen)
    assert yielded is storage_effect


def test_enqueue_wakes_one_parked_worker():
    """Proves an enqueue cast acks exactly one parked worker so it fetches the job."""

    lot = make_lot()
    lot.workers[bytes(WORKER_PID)] = WORKER_CALLER
    other = Caller(sender=Pid(node_id=0, id=3), ref=30)
    lot.workers[bytes(other.sender)] = other

    enqueue = EStorageEnqueue(job=JobSpec(fn_name="job"))
    effects, _ = drain_to(_handle_cast(lot, enqueue))

    assert sent_replies(effects) == [ReplyMsg(ref=10, body=WorkItemTag.NO_WORK)]
    assert list(lot.workers) == [bytes(other.sender)]


def test_job_done_wakes_the_parked_parent():
    """Proves a job-done cast acks the specific parked parent worker awaiting the result."""

    lot = make_lot()
    lot.workers[bytes(WORKER_PID)] = WORKER_CALLER

    done = EStorageJobDone(reply_to=bytes(WORKER_PID), sequence_number=0, body="result")
    effects, _ = drain_to(_handle_cast(lot, done), responses={EStorageJobDone: False})

    assert sent_replies(effects) == [ReplyMsg(ref=10, body=WorkItemTag.NO_WORK)]
    assert lot.workers == {}


def test_job_done_wakes_completion_waiters_when_finished():
    """Proves the final job-done cast replies True to every parked completion waiter."""

    lot = make_lot()
    lot.completion[bytes(WAITER_PID)] = WAITER_CALLER

    done = EStorageJobDone(reply_to=None, sequence_number=None, body="root result")
    effects, _ = drain_to(_handle_cast(lot, done), responses={EStorageJobDone: True})

    assert sent_replies(effects) == [ReplyMsg(ref=20, body=True)]
    assert lot.completion == {}


def test_job_done_keeps_waiters_parked_while_pending():
    """Proves completion waiters stay parked while jobs remain pending."""

    lot = make_lot()
    lot.completion[bytes(WAITER_PID)] = WAITER_CALLER

    done = EStorageJobDone(reply_to=None, sequence_number=None, body="mid result")
    effects, _ = drain_to(_handle_cast(lot, done), responses={EStorageJobDone: False})

    assert sent_replies(effects) == []
    assert lot.completion == {bytes(WAITER_PID): WAITER_CALLER}


# telemetry decoration


def emitted_park_tags(effects: list) -> list[tuple[str, str]]:
    return [
        (effect.body.dim("tag"), effect.body.dim("kind"))
        for effect in effects
        if isinstance(effect, EEmit)
    ]


def test_emitting_parks_marks_parked_workers():
    """Proves the call decorator emits a parked event when a worker is parked."""

    lot = make_lot()
    gen = emitting_parks(_handle_call, lot, EStorageGetJob(bytes(WORKER_PID)), WORKER_CALLER)
    effects, (_, reply) = drain_to(gen)

    assert reply is NoReply
    assert emitted_park_tags(effects) == [(ParkTag.PARKED, "worker")]


def test_emitting_parks_is_silent_when_replying():
    """Proves the call decorator emits nothing when the call is answered directly."""

    lot = make_lot()
    work = JobSpec(fn_name="fn")
    gen = emitting_parks(_handle_call, lot, EStorageGetJob(bytes(WORKER_PID)), WORKER_CALLER)
    effects, (_, reply) = drain_to(gen, responses={EStorageGetJob: work})

    assert reply == work
    assert emitted_park_tags(effects) == []


def test_emitting_wakes_marks_woken_workers():
    """Proves the cast decorator emits a woken event alongside each wake reply."""

    lot = make_lot()
    lot.workers[bytes(WORKER_PID)] = WORKER_CALLER

    enqueue = EStorageEnqueue(job=JobSpec(fn_name="job"))
    effects, _ = drain_to(emitting_wakes(_handle_cast, lot, enqueue))

    assert emitted_park_tags(effects) == [(ParkTag.WOKEN, "worker")]
    assert sent_replies(effects) == [ReplyMsg(ref=10, body=WorkItemTag.NO_WORK)]


def test_emitting_wakes_marks_completion_wakes():
    """Proves the cast decorator tags completion wakes distinctly from worker wakes."""

    lot = make_lot()
    lot.completion[bytes(WAITER_PID)] = WAITER_CALLER

    done = EStorageJobDone(reply_to=None, sequence_number=None, body="root result")
    wakes_gen = emitting_wakes(_handle_cast, lot, done)
    effects, _ = drain_to(wakes_gen, responses={EStorageJobDone: True})

    assert emitted_park_tags(effects) == [(ParkTag.WOKEN, "completion")]


# round-trip via handle() + memory storage handlers


def _make_handlers():
    """Shared storage handler set for round-trip tests."""
    return make_memory_storage_handlers()


def test_init_cast_seeds_backend_via_storage_handlers():
    """Proves EStorageEnqueue sent as a cast seeds the backend correctly."""

    handlers = _make_handlers()
    enqueue_effect = EStorageEnqueue(job=JobSpec(fn_name="start", args=(1, 2)))
    complete(handle(_handle_cast(make_lot(), enqueue_effect), handlers))
    is_done_call = _handle_call(make_lot(), EStorageIsDone(), WAITER_CALLER)
    _, result = complete(handle(is_done_call, handlers))
    assert result is NoReply


def test_round_trip_enqueue_then_get_job():
    """Proves a job enqueued via a cast storage effect is retrievable via a call storage effect."""

    handlers = _make_handlers()
    lot = make_lot()

    # enqueue root job, enqueue a child job, then fetch them
    root_enqueue = EStorageEnqueue(job=JobSpec(fn_name="root"))
    complete(handle(_handle_cast(lot, root_enqueue), handlers))
    child_enqueue = EStorageEnqueue(job=JobSpec(fn_name="child", args=(42,)))
    complete(handle(_handle_cast(lot, child_enqueue), handlers))
    # root job is first in queue; consume it, then ack its lease to fetch the child
    get_job = EStorageGetJob(b"worker")
    _, (root_lease_id, _root_work) = complete(
        handle(_handle_call(lot, get_job, WORKER_CALLER), handlers)
    )
    ack_get_job = EStorageGetJob(b"worker", ack=root_lease_id)
    _, (_, work) = complete(handle(_handle_call(lot, ack_get_job, WORKER_CALLER), handlers))
    assert work.fn_name == "child"
