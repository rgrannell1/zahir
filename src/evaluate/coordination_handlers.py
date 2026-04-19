from collections.abc import Generator
from dataclasses import dataclass
from functools import partial
from typing import Any

from tertius import ESend, ESelf, ESleep, Pid, mcall, mcast

from constants import ENQUEUE, GET_JOB, JOB_DONE, RELEASE, WORKER_POLL_MS
from effects import (
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
    yield from mcast(context.overseer, (ENQUEUE, effect.fn_name, effect.args, bytes(me), effect.timeout_ms, effect.nonce))


def _handle_get_job(
    context: CoordinationHandlerContext, effect: EGetJob
) -> Generator[Any, Any, Any]:
    """Poll the overseer for a job, sleeping between attempts until one is available."""

    while True:
        job = yield from mcall(context.overseer, GET_JOB)
        if job is not None:
            return job
        # no job available yet — sleep before retrying
        yield ESleep(ms=WORKER_POLL_MS)


def _handle_job_complete(
    context: CoordinationHandlerContext, effect: EJobComplete
) -> Generator[Any, Any, None]:
    """Route the result to the caller and inform the overseer the job is done."""

    if effect.reply_to is not None:
        yield ESend(Pid.from_bytes(effect.reply_to), (effect.nonce, effect.result))
    yield from mcast(context.overseer, JOB_DONE)


def _handle_job_fail(
    context: CoordinationHandlerContext, effect: EJobFail
) -> Generator[Any, Any, None]:
    """Route the failure to the caller and inform the overseer the job is done."""

    if effect.reply_to is not None:
        yield ESend(Pid.from_bytes(effect.reply_to), (effect.nonce, effect.error))
        yield from mcast(context.overseer, JOB_DONE)
    else:
        yield from mcast(context.overseer, (JOB_DONE, effect.error))


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
