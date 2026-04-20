from collections.abc import Generator, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from functools import partial, reduce
from typing import Any

from zahir.core.constants import BLOCKED_EFFECTS, THROWABLE
from zahir.core.effects import (
    EAcquire,
    EAcquireSlot,
    EGetSemaphore,
    EImpossible,
    ESatisfied,
    ESetSemaphore,
    ESetSemaphoreState,
    ESignal,
    ZahirCoordinationEffect,
)
from zahir.core.exceptions import InvalidEffect, JobError, JobTimeout


@dataclass
class JobHandlerContext:
    acquired: list[str] = field(default_factory=list)
    handler_wrappers: Sequence = field(default_factory=list)


def evaluate_job(
    job_gen: Generator[Any, Any, Any],
    context: JobHandlerContext,
    deadline: datetime | None,
) -> Generator[Any, Any, Any]:
    handlers = make_job_handlers(context)
    handler_value: Any = None
    pending_throw: Exception | None = None

    while True:
        try:
            # Throw if pending back into the job
            effect = (
                job_gen.throw(pending_throw)
                if pending_throw
                else job_gen.send(handler_value)
            )
            pending_throw = None
        except StopIteration as exc:
            # Job is done, return the value
            return exc.value

        # Check for timeout before processing the effect
        if deadline and datetime.now(UTC) >= deadline:
            pending_throw = JobTimeout()
            continue

        try:
            if not hasattr(effect, "tag"):
                raise InvalidEffect(f"job yielded non-Effect value: {effect!r}")
            elif effect.tag in handlers:
                handler_value = yield from handlers[effect.tag](effect)
            elif isinstance(effect, ZahirCoordinationEffect):
                raise InvalidEffect(
                    f"{type(effect).__name__} cannot be yielded directly in a job"
                )
            elif isinstance(effect, BLOCKED_EFFECTS):
                raise InvalidEffect(
                    f"{type(effect).__name__} cannot be yielded directly in a job"
                )
            else:
                # EAwait and EAwaitAll fall through here; the worker body intercepts them
                handler_value = yield effect
        except THROWABLE as exc:
            pending_throw = exc


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


def _handle_acquire(
    context: JobHandlerContext, effect: EAcquire
) -> Generator[Any, Any, bool]:
    """Attempt to acquire a concurrency slot, returning True on success and False on failure."""

    result = yield EAcquireSlot(name=effect.name, limit=effect.limit)

    if result:
        context.acquired.append(effect.name)
    return result


def _handle_signal(
    context: JobHandlerContext, effect: EGetSemaphore
) -> Generator[Any, Any, str]:
    """Get the current state of a named semaphore."""

    return (yield ESignal(name=effect.name))


def _handle_set_semaphore(
    context: JobHandlerContext, effect: ESetSemaphore
) -> Generator[Any, Any, None]:
    """Set the state of a semaphore and release all waiting workers."""

    yield ESetSemaphoreState(name=effect.name, state=effect.state)


def make_job_handlers(context: JobHandlerContext) -> dict[str, Any]:
    """Create job-effect handlers keyed by effect tag, with any user-supplied wrappers applied."""

    handlers = {
        ESatisfied.tag: partial(_handle_event, context),
        EImpossible.tag: partial(_handle_event, context),
        EAcquire.tag: partial(_handle_acquire, context),
        EGetSemaphore.tag: partial(_handle_signal, context),
        ESetSemaphore.tag: partial(_handle_set_semaphore, context),
    }
    return {
        tag: reduce(lambda h, w: w(h), context.handler_wrappers, h)
        for tag, h in handlers.items()
    }
