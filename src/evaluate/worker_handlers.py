from collections.abc import Generator
from functools import partial
from typing import Any

from tertius import EReceive, ESelf, ESend, ESleep, Pid, mcall, mcast

from constants import ACQUIRE, ENQUEUE, GET_JOB, JOB_DONE, RELEASE, SET_SEMAPHORE, SIGNAL, WORKER_POLL_MS
from effects import (
    EAcquire,
    EAwait,
    EAwaitAll,
    EGetJob,
    EImpossible,
    EJobComplete,
    EJobFail,
    ESatisfied,
    ESetSemaphore,
    ESignal,
)
from exceptions import JobError, JobTimeout


def _handle_event(
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


def _handle_await(overseer: Pid, effect: EAwait) -> Generator[Any, Any, Any]:
    """Enqueue the awaited job and wait for the reply, unwrapping it and raising appropriate exceptions on failure."""

    me: Pid = yield ESelf()
    # enqueue the job with the overseer, using our own pid as the reply_to so we get the response back when it's done
    yield from mcast(
        overseer,
        (ENQUEUE, effect.fn_name, effect.args, bytes(me), effect.timeout_ms, None),
    )

    # wait for the reply and unwrap it, raising exceptions as needed
    envelope = yield EReceive()
    nonce, body = envelope.body

    return _unwrap_reply(body)


def _handle_await_all(
    overseer: Pid, effect: EAwaitAll
) -> Generator[Any, Any, list[Any]]:
    """Enqueue multiple awaited jobs and wait for their replies, unwrapping them and raising appropriate exceptions on failure."""

    me: Pid = yield ESelf()

    # enqueue each
    for idx, eff in enumerate(effect.effects):
        yield from mcast(
            overseer, (ENQUEUE, eff.fn_name, eff.args, bytes(me), eff.timeout_ms, idx)
        )

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
    overseer: Pid, acquired: list[str], effect: EAcquire
) -> Generator[Any, Any, bool]:
    """Attempt to acquire a semaphore, returning True on success and False on failure."""

    result = yield from mcall(overseer, (ACQUIRE, effect.name, effect.limit))

    if result:
        acquired.append(effect.name)
    return result


def _handle_signal(overseer: Pid, effect: ESignal) -> Generator[Any, Any, str]:
    """Get the current state of a named semaphore."""

    return (yield from mcall(overseer, (SIGNAL, effect.name)))


def _handle_set_semaphore(
    overseer: Pid, effect: ESetSemaphore
) -> Generator[Any, Any, None]:
    """Set the state of a semaphore and release all waiting workers."""

    yield from mcast(overseer, (SET_SEMAPHORE, effect.name, effect.state))


def _handle_get_job(overseer: Pid, effect: EGetJob) -> Generator[Any, Any, Any]:
    """Poll the overseer for a job, sleeping between attempts until one is available."""

    while True:
        job = yield from mcall(overseer, GET_JOB)
        if job is not None:
            return job
        # no job available yet — sleep before retrying
        yield ESleep(ms=WORKER_POLL_MS)


def _handle_job_complete(
    overseer: Pid, effect: EJobComplete
) -> Generator[Any, Any, None]:
    """Route the result to the caller and inform the overseer the job is done."""

    if effect.reply_to is not None:
        yield ESend(Pid.from_bytes(effect.reply_to), (effect.nonce, effect.result))
    yield from mcast(overseer, JOB_DONE)


def _handle_job_fail(overseer: Pid, effect: EJobFail) -> Generator[Any, Any, None]:
    """Route the failure to the caller and inform the overseer the job is done."""

    if effect.reply_to is not None:
        yield ESend(Pid.from_bytes(effect.reply_to), (effect.nonce, effect.error))
        yield from mcast(overseer, JOB_DONE)
    else:
        yield from mcast(overseer, (JOB_DONE, effect.error))


def make_coordination_handlers(overseer: Pid) -> dict[str, Any]:
    """Coordination handlers for the worker process — intercept job lifecycle effects."""

    return {
        EGetJob.tag: partial(_handle_get_job, overseer),
        EJobComplete.tag: partial(_handle_job_complete, overseer),
        EJobFail.tag: partial(_handle_job_fail, overseer),
    }


def make_handlers(overseer: Pid, acquired: list[str]) -> dict[type, Any]:
    """Create worker handlers"""

    return {
        ESatisfied: _handle_event,
        EImpossible: _handle_event,
        EAwait: partial(_handle_await, overseer),
        EAwaitAll: partial(_handle_await_all, overseer),
        EAcquire: partial(_handle_acquire, overseer, acquired),
        ESignal: partial(_handle_signal, overseer),
        ESetSemaphore: partial(_handle_set_semaphore, overseer),
    }
