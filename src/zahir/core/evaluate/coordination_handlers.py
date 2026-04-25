from collections.abc import Generator, Sequence
from dataclasses import dataclass, field
from functools import partial, reduce
from typing import Any

from tertius import ESleep, Pid, mcall, mcast

from zahir.core.effects import (
    EAcquireSlot,
    EEnqueue,
    EGetError,
    EGetJob,
    EGetResult,
    EIsDone,
    EJobComplete,
    EJobFail,
    ERelease,
    ESetSemaphoreState,
    ESignal,
    EStorageAcquire,
    EStorageEnqueue,
    EStorageGetError,
    EStorageGetJob,
    EStorageGetResult,
    EStorageIsDone,
    EStorageJobDone,
    EStorageJobFailed,
    EStorageRelease,
    EStorageSetSemaphore,
    EStorageSignal,
)
from zahir.core.zahir_types import HandlerMap


@dataclass
class CoordinationHandlerContext:
    overseer: Pid
    handler_wrappers: Sequence = field(default_factory=list)


def _handle_enqueue(
    context: CoordinationHandlerContext, effect: EEnqueue
) -> Generator[Any, Any, None]:
    """Enqueue a child job with the overseer, routing the reply back to this worker."""

    yield from mcast(
        context.overseer,
        EStorageEnqueue(
            fn_name=effect.fn_name,
            args=effect.args,
            reply_to=effect.reply_to,
            timeout_ms=effect.timeout_ms,
            sequence_number=effect.sequence_number,
        ),
    )


def _handle_get_job(
    context: CoordinationHandlerContext, effect: EGetJob
) -> Generator[Any, Any, Any]:
    """Ask the overseer for work — returns a new job, a buffered result, or None."""

    return (yield from mcall(context.overseer, EStorageGetJob(effect.worker_pid_bytes)))


def _handle_job_complete(
    context: CoordinationHandlerContext, effect: EJobComplete
) -> Generator[Any, Any, None]:
    """Route the result to the parent worker via the overseer and decrement pending."""

    yield from mcast(
        context.overseer,
        EStorageJobDone(
            reply_to=effect.reply_to,
            sequence_number=effect.sequence_number,
            body=effect.result,
        ),
    )


def _handle_job_fail(
    context: CoordinationHandlerContext, effect: EJobFail
) -> Generator[Any, Any, None]:
    """Route the failure to the parent worker via the overseer, or record it as root error."""

    if effect.reply_to is not None:
        yield from mcast(
            context.overseer,
            EStorageJobDone(
                reply_to=effect.reply_to,
                sequence_number=effect.sequence_number,
                body=effect.error,
            ),
        )
    else:
        yield from mcast(context.overseer, EStorageJobFailed(error=effect.error))


def _handle_release(
    context: CoordinationHandlerContext, effect: ERelease
) -> Generator[Any, Any, None]:
    """Release a named concurrency slot back to the overseer."""

    yield from mcast(context.overseer, EStorageRelease(name=effect.name))


def _handle_acquire_slot(
    context: CoordinationHandlerContext, effect: EAcquireSlot
) -> Generator[Any, Any, bool]:
    """Request a named concurrency slot from the overseer."""

    return (yield from mcall(context.overseer, EStorageAcquire(name=effect.name, limit=effect.limit)))


def _handle_signal(
    context: CoordinationHandlerContext, effect: ESignal
) -> Generator[Any, Any, str]:
    """Query the current state of a named semaphore from the overseer."""

    return (yield from mcall(context.overseer, EStorageSignal(name=effect.name)))


def _handle_set_semaphore_state(
    context: CoordinationHandlerContext, effect: ESetSemaphoreState
) -> Generator[Any, Any, None]:
    """Write a new semaphore state to the overseer."""

    yield from mcast(context.overseer, EStorageSetSemaphore(name=effect.name, state=effect.state))


def _handle_is_done(
    context: CoordinationHandlerContext, effect: EIsDone
) -> Generator[Any, Any, bool]:
    """Ask the overseer whether all pending jobs have completed."""

    return (yield from mcall(context.overseer, EStorageIsDone()))


def _handle_get_error(
    context: CoordinationHandlerContext, effect: EGetError
) -> Generator[Any, Any, Exception | None]:
    """Retrieve the root error from the overseer, if any."""

    return (yield from mcall(context.overseer, EStorageGetError()))


def _handle_get_result(
    context: CoordinationHandlerContext, effect: EGetResult
) -> Generator[Any, Any, Any]:
    """Retrieve the root job's return value from the overseer."""

    return (yield from mcall(context.overseer, EStorageGetResult()))


def make_coordination_handlers(context: CoordinationHandlerContext) -> HandlerMap:
    """Create handlers for all coordination effects — job lifecycle (worker) and completion polling (root)."""

    handlers = {
        EAcquireSlot.tag: partial(_handle_acquire_slot, context),
        EEnqueue.tag: partial(_handle_enqueue, context),
        EGetError.tag: partial(_handle_get_error, context),
        EGetJob.tag: partial(_handle_get_job, context),
        EGetResult.tag: partial(_handle_get_result, context),
        EIsDone.tag: partial(_handle_is_done, context),
        EJobComplete.tag: partial(_handle_job_complete, context),
        EJobFail.tag: partial(_handle_job_fail, context),
        ERelease.tag: partial(_handle_release, context),
        ESetSemaphoreState.tag: partial(_handle_set_semaphore_state, context),
        ESignal.tag: partial(_handle_signal, context),
    }
    return {
        tag: reduce(lambda h, w: w(h), context.handler_wrappers, h)
        for tag, h in handlers.items()
    }
