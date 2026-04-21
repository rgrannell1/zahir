from collections.abc import Generator
from dataclasses import dataclass
from functools import partial
from typing import Any

from tertius import ESelf, ESleep, Pid, mcall, mcast

from zahir.core.constants import (
    ENQUEUE,
    GET_JOB,
    JOB_DONE,
    JOB_FAILED,
    RELEASE,
    WORKER_POLL_MS,
)
from zahir.core.effects import (
    EEnqueue,
    EGetJob,
    EJobComplete,
    EJobFail,
    ERelease,
)


@dataclass
class CoordinationHandlerContext:
    overseer: Pid


def _handle_enqueue(
    context: CoordinationHandlerContext, effect: EEnqueue
) -> Generator[Any, Any, None]:
    """Enqueue a child job with the overseer, routing the reply back to this worker."""

    me: Pid = yield ESelf()
    yield from mcast(
        context.overseer,
        (
            ENQUEUE,
            effect.fn_name,
            effect.args,
            bytes(me),
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
        context.overseer, (JOB_DONE, effect.reply_to, effect.sequence_number, effect.result)
    )


def _handle_job_fail(
    context: CoordinationHandlerContext, effect: EJobFail
) -> Generator[Any, Any, None]:
    """Route the failure to the parent worker via the overseer, or record it as root error."""

    if effect.reply_to is not None:
        yield from mcast(
            context.overseer, (JOB_DONE, effect.reply_to, effect.sequence_number, effect.error)
        )
    else:
        yield from mcast(context.overseer, (JOB_FAILED, effect.error))


def _handle_release(
    context: CoordinationHandlerContext, effect: ERelease
) -> Generator[Any, Any, None]:
    """Release a named concurrency slot back to the overseer."""

    yield from mcast(context.overseer, (RELEASE, effect.name))


def make_coordination_handlers(context: CoordinationHandlerContext) -> dict[str, Any]:
    """Create coordination handlers for the worker process — intercept job lifecycle effects."""

    return {
        EEnqueue.tag: partial(_handle_enqueue, context),
        EGetJob.tag: partial(_handle_get_job, context),
        EJobComplete.tag: partial(_handle_job_complete, context),
        EJobFail.tag: partial(_handle_job_fail, context),
        ERelease.tag: partial(_handle_release, context),
    }
