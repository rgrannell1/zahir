from collections.abc import Generator, Sequence
from dataclasses import dataclass, field
from functools import partial, reduce
from typing import Any

from tertius import ESleep, Pid, mcall, mcast

from zahir.core.constants import (
    ACQUIRE,
    ENQUEUE,
    GET_ERROR,
    GET_JOB,
    GET_RESULT,
    IS_DONE,
    JOB_DONE,
    JOB_FAILED,
    RELEASE,
    SET_SEMAPHORE,
    SIGNAL,
)
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
)


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
        (
            ENQUEUE,
            effect.fn_name,
            effect.args,
            effect.reply_to,
            effect.timeout_ms,
            effect.sequence_number,
        ),
    )


def _handle_get_job(
    context: CoordinationHandlerContext, effect: EGetJob
) -> Generator[Any, Any, Any]:
    """Ask the overseer for work — returns a new job, a buffered result, or None."""

    return (yield from mcall(context.overseer, (GET_JOB, effect.worker_pid_bytes)))


def _handle_job_complete(
    context: CoordinationHandlerContext, effect: EJobComplete
) -> Generator[Any, Any, None]:
    """Route the result to the parent worker via the overseer and decrement pending."""

    yield from mcast(
        context.overseer,
        (JOB_DONE, effect.reply_to, effect.sequence_number, effect.result),
    )


def _handle_job_fail(
    context: CoordinationHandlerContext, effect: EJobFail
) -> Generator[Any, Any, None]:
    """Route the failure to the parent worker via the overseer, or record it as root error."""

    if effect.reply_to is not None:
        yield from mcast(
            context.overseer,
            (JOB_DONE, effect.reply_to, effect.sequence_number, effect.error),
        )
    else:
        yield from mcast(context.overseer, (JOB_FAILED, effect.error))


def _handle_release(
    context: CoordinationHandlerContext, effect: ERelease
) -> Generator[Any, Any, None]:
    """Release a named concurrency slot back to the overseer."""

    yield from mcast(context.overseer, (RELEASE, effect.name))


def _handle_acquire_slot(
    context: CoordinationHandlerContext, effect: EAcquireSlot
) -> Generator[Any, Any, bool]:
    """Request a named concurrency slot from the overseer."""

    return (yield from mcall(context.overseer, (ACQUIRE, effect.name, effect.limit)))


def _handle_signal(
    context: CoordinationHandlerContext, effect: ESignal
) -> Generator[Any, Any, str]:
    """Query the current state of a named semaphore from the overseer."""

    return (yield from mcall(context.overseer, (SIGNAL, effect.name)))


def _handle_set_semaphore_state(
    context: CoordinationHandlerContext, effect: ESetSemaphoreState
) -> Generator[Any, Any, None]:
    """Write a new semaphore state to the overseer."""

    yield from mcast(context.overseer, (SET_SEMAPHORE, effect.name, effect.state))


def _handle_is_done(
    context: CoordinationHandlerContext, effect: EIsDone
) -> Generator[Any, Any, bool]:
    """Ask the overseer whether all pending jobs have completed."""

    return (yield from mcall(context.overseer, IS_DONE))


def _handle_get_error(
    context: CoordinationHandlerContext, effect: EGetError
) -> Generator[Any, Any, Exception | None]:
    """Retrieve the root error from the overseer, if any."""

    return (yield from mcall(context.overseer, GET_ERROR))


def _handle_get_result(
    context: CoordinationHandlerContext, effect: EGetResult
) -> Generator[Any, Any, Any]:
    """Retrieve the root job's return value from the overseer."""

    return (yield from mcall(context.overseer, GET_RESULT))


def make_root_handlers(context: CoordinationHandlerContext) -> dict[str, Any]:
    """Create handlers for the root polling loop — check completion, errors, and the return value."""

    return {
        EIsDone.tag: partial(_handle_is_done, context),
        EGetError.tag: partial(_handle_get_error, context),
        EGetResult.tag: partial(_handle_get_result, context),
    }


def make_coordination_handlers(context: CoordinationHandlerContext) -> dict[str, Any]:
    """Create coordination handlers for the worker process — intercept job lifecycle effects."""

    handlers = {
        EAcquireSlot.tag: partial(_handle_acquire_slot, context),
        EEnqueue.tag: partial(_handle_enqueue, context),
        EGetJob.tag: partial(_handle_get_job, context),
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
