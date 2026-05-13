from collections.abc import Generator, Sequence
from dataclasses import dataclass, field
from functools import partial, reduce
from typing import Any, cast

from tertius import Pid, mcall, mcast

from zahir.core.combinators import apply_wrapper
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
    EStorageGetError,
    EStorageGetJob,
    EStorageGetResult,
    EStorageIsDone,
    EStorageJobDone,
    EStorageJobFailed,
    EStorageRelease,
    EStorageSetState,
    EStorageGetState,
)
from zahir.core.zahir_types import CoordinationHandlerMap, HandlerMap


@dataclass
class CoordinationHandlerContext:
    """TODO this is a specialist class, is it needed?"""

    overseer: Pid
    handler_wrappers: Sequence = field(default_factory=list)


def make_merged_coordination_handlers(
    overseer: Pid,
    handler_wrappers: Sequence,
    user_handlers: HandlerMap,
) -> CoordinationHandlerMap:
    """Coordination handlers merged with user overrides.
    User-provided handlers take precedence, allowing any coordination handler to be replaced.
    """

    ctx = CoordinationHandlerContext(overseer=overseer, handler_wrappers=handler_wrappers)
    return cast(CoordinationHandlerMap, {**make_coordination_handlers(ctx), **user_handlers})


def _handle_enqueue(context: CoordinationHandlerContext, effect: EEnqueue) -> Generator[Any, Any, None]:
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


def _handle_get_job(context: CoordinationHandlerContext, effect: EGetJob) -> Generator[Any, Any, Any]:
    """Ask the overseer for work — returns a new job, a buffered result, or None."""

    return (yield from mcall(context.overseer, EStorageGetJob(effect.worker_pid_bytes)))


def _handle_job_complete(context: CoordinationHandlerContext, effect: EJobComplete) -> Generator[Any, Any, None]:
    """Route the result to the parent worker via the overseer and decrement pending."""

    yield from mcast(
        context.overseer,
        EStorageJobDone(
            reply_to=effect.reply_to,
            sequence_number=effect.sequence_number,
            body=effect.result,
        ),
    )


def _handle_job_fail(context: CoordinationHandlerContext, effect: EJobFail) -> Generator[Any, Any, None]:
    """Route the failure to the parent worker via the overseer, or record it as root error."""

    if effect.reply_to is None:
        yield from mcast(context.overseer, EStorageJobFailed(error=effect.error))
        return

    yield from mcast(
        context.overseer,
        EStorageJobDone(
            reply_to=effect.reply_to,
            sequence_number=effect.sequence_number,
            body=effect.error,
        ),
    )


def _handle_release(context: CoordinationHandlerContext, effect: ERelease) -> Generator[Any, Any, None]:
    """Release a named concurrency slot back to the overseer."""

    yield from mcast(context.overseer, EStorageRelease(name=effect.name))


def _handle_acquire_slot(context: CoordinationHandlerContext, effect: EAcquireSlot) -> Generator[Any, Any, bool]:
    """Request a named concurrency slot from the overseer."""

    return (yield from mcall(context.overseer, EStorageAcquire(name=effect.name, limit=effect.limit)))


def _handle_get_state(context: CoordinationHandlerContext, effect: EGetState) -> Generator[Any, Any, str]:
    """Read a value from the overseer's key-value store by name."""

    return (yield from mcall(context.overseer, EStorageGetState(name=effect.name)))


def _handle_set_state(context: CoordinationHandlerContext, effect: ESetState) -> Generator[Any, Any, None]:
    """Write a value to the overseer's key-value store by name."""

    yield from mcast(context.overseer, EStorageSetState(name=effect.name, state=effect.value))


def _handle_is_done(context: CoordinationHandlerContext, effect: EIsDone) -> Generator[Any, Any, bool]:
    """Ask the overseer whether all pending jobs have completed."""

    return (yield from mcall(context.overseer, EStorageIsDone()))


def _handle_get_error(context: CoordinationHandlerContext, effect: EGetError) -> Generator[Any, Any, Exception | None]:
    """Retrieve the root error from the overseer, if any."""

    return (yield from mcall(context.overseer, EStorageGetError()))


def _handle_get_result(context: CoordinationHandlerContext, effect: EGetResult) -> Generator[Any, Any, Any]:
    """Retrieve the root job's return value from the overseer."""

    return (yield from mcall(context.overseer, EStorageGetResult()))


# EGetJob fires on every worker poll tick — wrapping it generates high-volume noise with no signal.
# TODO encode this skip in the telemetry layer
_SKIP_WRAP = {EGetJob.tag}


# Now, assemble all of the handlers and wrap them with telemetry context.
def make_coordination_handlers(context: CoordinationHandlerContext) -> CoordinationHandlerMap:
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
        EGetState.tag: partial(_handle_get_state, context),
        ESetState.tag: partial(_handle_set_state, context),
    }

    mapped = {}
    wrappers = context.handler_wrappers

    for tag, handler in handlers.items():
        if tag in _SKIP_WRAP:
            mapped[tag] = handler
            continue

        mapped[tag] = reduce(apply_wrapper, wrappers, handler)

    return cast(CoordinationHandlerMap, mapped)
