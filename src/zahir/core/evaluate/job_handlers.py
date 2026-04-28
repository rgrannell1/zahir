"""Per-job effect handlers: acquire, semaphore read/write, and the job/timeout guard stack."""

from collections.abc import Generator, Sequence
from datetime import UTC, datetime
from functools import partial, reduce
from typing import Any

from zahir.core.combinators import apply_wrapper
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
from zahir.core.evaluate.suspension import _WorkerLocals
from zahir.core.exceptions import InvalidEffectError, JobTimeoutError
from zahir.core.zahir_types import HandlerMap


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
    handlers: HandlerMap,
    deadline: datetime | None,
) -> Generator[Any, Any, Any]:
    """Wrap job_gen in job_guard and timeout_guard using pre-built handlers."""
    guarded = job_guard(job_gen, handlers)
    return timeout_guard(guarded, deadline)


def _handle_acquire(
    locals_: _WorkerLocals, effect: EAcquire
) -> Generator[Any, Any, bool]:
    """Attempt to acquire a concurrency slot, returning True on success and False on failure."""

    result = yield EAcquireSlot(name=effect.name, limit=effect.limit)

    if result:
        locals_.current_job.acquired.append(effect.name)
    return result


def _handle_signal(effect: EGetSemaphore) -> Generator[Any, Any, str]:
    """Get the current state of a named semaphore."""

    return (yield ESignal(name=effect.name))


def _handle_set_semaphore(effect: ESetSemaphore) -> Generator[Any, Any, None]:
    """Set the state of a semaphore and release all waiting workers."""

    yield ESetSemaphoreState(name=effect.name, state=effect.state)


def make_job_handlers(locals_: _WorkerLocals, handler_wrappers: Sequence) -> HandlerMap:
    """Create job-effect handlers keyed by effect tag, with any user-supplied wrappers applied."""

    handlers = {
        EAcquire.tag: partial(_handle_acquire, locals_),
        EGetSemaphore.tag: _handle_signal,
        ESetSemaphore.tag: _handle_set_semaphore,
    }
    return {
        tag: reduce(apply_wrapper, handler_wrappers, handler)
        for tag, handler in handlers.items()
    }
