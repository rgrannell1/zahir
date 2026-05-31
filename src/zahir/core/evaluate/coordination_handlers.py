from collections.abc import Generator, Sequence
from functools import partial
from typing import Any, cast

from tertius import Pid, mcall, mcast

from zahir.core.combinators import build_handler_map, merge_handlers
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
    EStorageGetState,
    EStorageIsDone,
    EStorageJobDone,
    EStorageJobFailed,
    EStorageRelease,
    EStorageSetState,
)
from zahir.core.zahir_types import CoordinationHandlerMap, HandlerMap


def _handle_enqueue(
    overseer: Pid, effect: EEnqueue
) -> Generator[Any, Any, None]:
    """Enqueue a child job with the overseer, routing the reply back to this worker."""

    yield from mcast(
        overseer,
        EStorageEnqueue(
            fn_name=effect.fn_name,
            args=effect.args,
            reply_to=effect.reply_to,
            timeout_ms=effect.timeout_ms,
            sequence_number=effect.sequence_number,
        ),
    )


def _handle_get_job(
    overseer: Pid, effect: EGetJob
) -> Generator[Any, Any, Any]:
    """Ask the overseer for work — returns a new job, a buffered result, or None."""

    return (yield from mcall(overseer, EStorageGetJob(effect.worker_pid_bytes)))


def _handle_job_complete(
    overseer: Pid, effect: EJobComplete
) -> Generator[Any, Any, None]:
    """Route the result to the parent worker via the overseer and decrement pending."""

    yield from mcast(
        overseer,
        EStorageJobDone(
            reply_to=effect.reply_to,
            sequence_number=effect.sequence_number,
            body=effect.result,
        ),
    )


def _handle_job_fail(
    overseer: Pid, effect: EJobFail
) -> Generator[Any, Any, None]:
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


def _handle_release(
    overseer: Pid, effect: ERelease
) -> Generator[Any, Any, None]:
    """Release a named concurrency slot back to the overseer."""

    yield from mcast(overseer, EStorageRelease(name=effect.name))


def _handle_acquire_slot(
    overseer: Pid, effect: EAcquireSlot
) -> Generator[Any, Any, bool]:
    """Request a named concurrency slot from the overseer."""

    acquire_effect = EStorageAcquire(name=effect.name, limit=effect.limit)
    return (yield from mcall(overseer, acquire_effect))


def _handle_get_state(
    overseer: Pid, effect: EGetState
) -> Generator[Any, Any, str]:
    """Read a value from the overseer's key-value store by name."""

    return (yield from mcall(overseer, EStorageGetState(name=effect.name)))


def _handle_set_state(
    overseer: Pid, effect: ESetState
) -> Generator[Any, Any, None]:
    """Write a value to the overseer's key-value store by name."""

    yield from mcast(overseer, EStorageSetState(name=effect.name, state=effect.value))


def _handle_is_done(
    overseer: Pid, effect: EIsDone
) -> Generator[Any, Any, bool]:
    """Ask the overseer whether all pending jobs have completed."""

    return (yield from mcall(overseer, EStorageIsDone()))


def _handle_get_error(
    overseer: Pid, effect: EGetError
) -> Generator[Any, Any, Exception | None]:
    """Retrieve the root error from the overseer, if any."""

    return (yield from mcall(overseer, EStorageGetError()))


def _handle_get_result(
    overseer: Pid, effect: EGetResult
) -> Generator[Any, Any, Any]:
    """Retrieve the root job's return value from the overseer."""

    return (yield from mcall(overseer, EStorageGetResult()))


# EGetJob fires on every worker poll tick — wrapping it generates high-volume noise with no signal.
# TODO encode this skip in the telemetry layer
_SKIP_WRAP = frozenset({EGetJob.tag})


# Now, assemble all of the handlers and wrap them with telemetry context.
def make_merged_coordination_handlers(
    overseer: Pid,
    handler_wrappers: Sequence,
    user_handlers: HandlerMap,
) -> CoordinationHandlerMap:
    """Create handlers for all coordination effects, merged with user overrides.

    User-provided handlers take precedence, allowing any coordination handler to be replaced.
    Handles job lifecycle (worker) and completion polling (root).
    """

    bindings = {
        EAcquireSlot.tag: partial(_handle_acquire_slot, overseer),
        EEnqueue.tag: partial(_handle_enqueue, overseer),
        EGetError.tag: partial(_handle_get_error, overseer),
        EGetJob.tag: partial(_handle_get_job, overseer),
        EGetResult.tag: partial(_handle_get_result, overseer),
        EIsDone.tag: partial(_handle_is_done, overseer),
        EJobComplete.tag: partial(_handle_job_complete, overseer),
        EJobFail.tag: partial(_handle_job_fail, overseer),
        ERelease.tag: partial(_handle_release, overseer),
        EGetState.tag: partial(_handle_get_state, overseer),
        ESetState.tag: partial(_handle_set_state, overseer),
    }

    base = build_handler_map(bindings, handler_wrappers, skip=_SKIP_WRAP)
    return cast(CoordinationHandlerMap, merge_handlers(base, user_handlers))
