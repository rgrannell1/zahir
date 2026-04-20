from collections.abc import Generator
from dataclasses import dataclass
from functools import partial
from typing import Any

from tertius import ESleep, Pid, mcall, mcast

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


@dataclass
class CoordinationHandlerContext:
    overseer: Pid


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
            effect.nonce,
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
        context.overseer, (JOB_DONE, effect.reply_to, effect.nonce, effect.result)
    )


def _handle_job_fail(
    context: CoordinationHandlerContext, effect: EJobFail
) -> Generator[Any, Any, None]:
    """Route the failure to the parent worker via the overseer, or record it as root error."""

    if effect.reply_to is not None:
        yield from mcast(
            context.overseer, (JOB_DONE, effect.reply_to, effect.nonce, effect.error)
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


def make_coordination_handlers(context: CoordinationHandlerContext) -> dict[str, Any]:
    """Create coordination handlers for the worker process — intercept job lifecycle effects."""

    return {
        EAcquireSlot.tag: partial(_handle_acquire_slot, context),
        EEnqueue.tag: partial(_handle_enqueue, context),
        EGetJob.tag: partial(_handle_get_job, context),
        EJobComplete.tag: partial(_handle_job_complete, context),
        EJobFail.tag: partial(_handle_job_fail, context),
        ERelease.tag: partial(_handle_release, context),
        ESetSemaphoreState.tag: partial(_handle_set_semaphore_state, context),
        ESignal.tag: partial(_handle_signal, context),
    }
