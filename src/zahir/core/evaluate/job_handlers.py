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
    ESetSemaphore,
    ESetSemaphoreState,
    ESignal,
    ZahirCoordinationEffect,
)
from zahir.core.exceptions import InvalidEffectError, JobError, JobTimeoutError
from zahir.core.zahir_types import HandlerMap


@dataclass
class JobHandlerContext:
    acquired: list[str] = field(default_factory=list)
    handler_wrappers: Sequence = field(default_factory=list)


def job_guard(gen: Generator, handlers: HandlerMap) -> Generator:
    """Drive gen: reject disallowed effects with InvalidEffectError, dispatch job-level effects to handlers, bubble the rest."""

    send_value: Any = None
    pending_throw: Exception | None = None

    while True:
        try:
            effect = gen.throw(pending_throw) if pending_throw else gen.send(send_value)
            pending_throw = None
        except StopIteration as stop:
            return stop.value

        if not hasattr(effect, "tag"):
            pending_throw = InvalidEffectError(f"job yielded non-Effect value: {effect!r}")
            continue
        if isinstance(effect, ZahirCoordinationEffect):
            pending_throw = InvalidEffectError(
                f"{type(effect).__name__} cannot be yielded directly in a job"
            )
            continue
        if isinstance(effect, BLOCKED_EFFECTS):
            pending_throw = InvalidEffectError(
                f"{type(effect).__name__} cannot be yielded directly in a job"
            )
            continue

        try:
            if effect.tag in handlers:
                send_value = yield from handlers[effect.tag](effect)
            else:
                send_value = yield effect
        except THROWABLE as exc:
            pending_throw = exc


def timeout_guard(gen: Generator, deadline: datetime | None) -> Generator:
    """Wrap gen, injecting JobTimeoutError into the job if the deadline passes between effects."""

    send_value: Any = None
    pending_throw: Exception | None = None

    while True:
        try:
            effect = gen.throw(pending_throw) if pending_throw else gen.send(send_value)
            pending_throw = None
        except StopIteration as stop:
            return stop.value

        if deadline and datetime.now(UTC) >= deadline:
            pending_throw = JobTimeoutError()
            continue

        try:
            send_value = yield effect
        except Exception as err:  # noqa: BLE001
            send_value = None
            pending_throw = err


def evaluate_job(
    job_gen: Generator[Any, Any, Any],
    context: JobHandlerContext,
    deadline: datetime | None,
) -> Generator[Any, Any, Any]:
    guarded = job_guard(job_gen, make_job_handlers(context))
    return timeout_guard(guarded, deadline)


def _unwrap_reply(body: Any) -> Any:
    """Unwrap the reply from a job, raising appropriate exceptions for timeouts or errors."""

    if isinstance(body, JobTimeoutError):
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


def apply_wrapper(handler: Any, wrapper: Any) -> Any:
    """Apply a single handler wrapper to a handler."""
    return wrapper(handler)


def make_job_handlers(context: JobHandlerContext) -> HandlerMap:
    """Create job-effect handlers keyed by effect tag, with any user-supplied wrappers applied."""

    handlers = {
        EAcquire.tag: partial(_handle_acquire, context),
        EGetSemaphore.tag: partial(_handle_signal, context),
        ESetSemaphore.tag: partial(_handle_set_semaphore, context),
    }
    return {
        tag: reduce(apply_wrapper, context.handler_wrappers, handler)
        for tag, handler in handlers.items()
    }
