from collections.abc import Generator
from dataclasses import dataclass, field
from functools import partial, reduce
from typing import Any

from tertius import EReceive, Pid, mcall, mcast

from zahir.core.constants import ACQUIRE, SET_SEMAPHORE, SIGNAL
from zahir.core.effects import (
    EAcquire,
    EAwait,
    EAwaitAll,
    EEnqueue,
    EImpossible,
    ESatisfied,
    ESetSemaphore,
    ESignal,
)
from zahir.core.exceptions import JobError, JobTimeout


@dataclass
class JobHandlerContext:
    overseer: Pid
    # acquired tracks concurrency slots taken during this job; it is a mutable
    # side-channel shared with _worker_body, which reads it after job completion
    # to yield ERelease effects. this coupling is a known limitation to be addressed.
    acquired: list[str] = field(default_factory=list)
    handler_wrappers: tuple = ()


def _handle_event(
    context: JobHandlerContext,
    effect: ESatisfied | EImpossible,
) -> Generator[Any, Any, ESatisfied | EImpossible]:
    """Simply re-emit the event to unblock waiting workers."""

    return effect
    yield


def _unwrap_reply(body: Any) -> Any:
    """Unwrap the reply from a job, raising appropriate exceptions for timeouts or errors."""

    if isinstance(body, JobTimeout):
        raise body

    if isinstance(body, JobError):
        raise body

    return body


def _handle_await(
    context: JobHandlerContext, effect: EAwait
) -> Generator[Any, Any, Any]:
    """Enqueue the awaited job and wait for the reply, unwrapping it and raising appropriate exceptions on failure."""

    # enqueue the job, routing the reply back to this worker
    yield EEnqueue(fn_name=effect.fn_name, args=effect.args, timeout_ms=effect.timeout_ms, nonce=None)

    # wait for the reply and unwrap it, raising exceptions as needed
    envelope = yield EReceive()
    _nonce, body = envelope.body

    return _unwrap_reply(body)


def _handle_await_all(
    context: JobHandlerContext, effect: EAwaitAll
) -> Generator[Any, Any, list[Any]]:
    """Enqueue multiple awaited jobs and wait for their replies, unwrapping them and raising appropriate exceptions on failure."""

    # enqueue each job, using the index as a nonce to correlate replies
    for idx, eff in enumerate(effect.effects):
        yield EEnqueue(fn_name=eff.fn_name, args=eff.args, timeout_ms=eff.timeout_ms, nonce=idx)

    # populate a blank list for the results
    results: list[Any] = [None] * len(effect.effects)
    first_error: Exception | None = None

    for _ in effect.effects:
        # wait for each reply and unwrap it, raising exceptions as needed. if multiple jobs error, we'll raise the first one we see.
        envelope = yield EReceive()
        nonce, body = envelope.body

        try:
            results[nonce] = _unwrap_reply(body)
        except (JobTimeout, JobError) as exc:
            # let's throw the first error we see.
            if first_error is None:
                first_error = exc

    # if there were any errors, raise the first one we saw. otherwise return the list of results.
    if first_error is not None:
        raise first_error

    return results


def _handle_acquire(
    context: JobHandlerContext, effect: EAcquire
) -> Generator[Any, Any, bool]:
    """Attempt to acquire a semaphore, returning True on success and False on failure."""

    result = yield from mcall(context.overseer, (ACQUIRE, effect.name, effect.limit))

    if result:
        context.acquired.append(effect.name)
    return result


def _handle_signal(
    context: JobHandlerContext, effect: ESignal
) -> Generator[Any, Any, str]:
    """Get the current state of a named semaphore."""

    return (yield from mcall(context.overseer, (SIGNAL, effect.name)))


def _handle_set_semaphore(
    context: JobHandlerContext, effect: ESetSemaphore
) -> Generator[Any, Any, None]:
    """Set the state of a semaphore and release all waiting workers."""

    yield from mcast(context.overseer, (SET_SEMAPHORE, effect.name, effect.state))


def make_job_handlers(context: JobHandlerContext) -> dict[str, Any]:
    """Create job-effect handlers keyed by effect tag, with any user-supplied wrappers applied."""

    handlers = {
        ESatisfied.tag: partial(_handle_event, context),
        EImpossible.tag: partial(_handle_event, context),
        EAwait.tag: partial(_handle_await, context),
        EAwaitAll.tag: partial(_handle_await_all, context),
        EAcquire.tag: partial(_handle_acquire, context),
        ESignal.tag: partial(_handle_signal, context),
        ESetSemaphore.tag: partial(_handle_set_semaphore, context),
    }
    return {
        tag: reduce(lambda h, w: w(h), context.handler_wrappers, h)
        for tag, h in handlers.items()
    }
