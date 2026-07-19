"""Per-job effect handlers: acquire, semaphore read/write, and the job/timeout guard stack."""

import time
from collections.abc import Generator, Sequence
from functools import partial
from typing import Any, cast

from zahir.core.combinators import build_handler_map
from zahir.core.constants import BLOCKED_EFFECTS, THROWABLE
from zahir.core.effects import (
    EAcquire,
    EReleaseSlot,
    EStorageAcquire,
    EStorageRelease,
    ZahirCoordinationEffect,
    ZahirStorageEffect,
)
from zahir.core.evaluate.suspension import WorkerLocals
from zahir.core.exceptions import InvalidEffectError, JobTimeoutError
from zahir.core.zahir_types import JobHandlerMap


def _validate_effect(effect) -> InvalidEffectError | None:
    """Return an error if effect is not allowed in a job context, else None."""
    if not hasattr(effect, "tag"):
        return InvalidEffectError(f"job yielded non-Effect value: {effect!r}")
    if isinstance(effect, ZahirCoordinationEffect | ZahirStorageEffect):
        return InvalidEffectError(f"{type(effect).__name__} cannot be yielded directly in a job")
    if isinstance(effect, BLOCKED_EFFECTS):
        return InvalidEffectError(f"{type(effect).__name__} cannot be yielded directly in a job")
    return None


def _advance(
    gen: Generator,
    send_value: Any,
    pending_throw: Exception | None,
) -> tuple[Any, bool, Any]:
    """Resume gen with a value or a throw; return (effect, done, return_value)."""

    try:
        effect = gen.throw(pending_throw) if pending_throw else gen.send(send_value)
        return (effect, False, None)
    except StopIteration as stop:
        return (None, True, stop.value)


def _dispatch(handlers: JobHandlerMap, effect: Any) -> Generator:
    """Route an effect to its job-level handler, or bubble it up."""

    if effect.tag in handlers:
        return (yield from handlers[effect.tag](effect))  # type: ignore[literal-required]
    return (yield effect)


def job_guard(gen: Generator, handlers: JobHandlerMap) -> Generator:
    """Drive gen: reject disallowed effects, dispatch job-level effects to
    handlers, bubble the rest."""

    send_value: Any = None
    pending_throw: Exception | None = None

    while True:
        effect, done, return_value = _advance(gen, send_value, pending_throw)
        if done:
            return return_value

        pending_throw = _validate_effect(effect)
        if pending_throw:
            continue

        try:
            send_value = yield from _dispatch(handlers, effect)
        except THROWABLE as exc:
            pending_throw = exc


def timeout_guard(gen: Generator, deadline: float | None) -> Generator:
    """Wrap gen, injecting JobTimeoutError into the job if the deadline passes between effects.

    deadline is a monotonic-clock instant (time.monotonic() seconds), so wall-clock
    steps from NTP or suspend/resume cannot fire it early or defer it.
    """

    send_value: Any = None
    pending_throw: Exception | None = None

    while True:
        effect, done, return_value = _advance(gen, send_value, pending_throw)
        if done:
            return return_value

        pending_throw = None
        if deadline is not None and time.monotonic() >= deadline:
            pending_throw = JobTimeoutError()
            continue

        try:
            send_value = yield effect
        except Exception as err:  # noqa: BLE001
            send_value, pending_throw = None, err


def evaluate_job(
    job_gen: Generator[Any, Any, Any],
    handlers: JobHandlerMap,
    deadline: float | None,
) -> Generator[Any, Any, Any]:
    """Wrap job_gen in job_guard and timeout_guard using pre-built handlers."""
    guarded = job_guard(job_gen, handlers)
    return timeout_guard(guarded, deadline)


def _handle_acquire(locals_: WorkerLocals, effect: EAcquire) -> Generator[Any, Any, bool]:
    """Attempt to acquire a concurrency slot, returning True on success and False on failure."""

    result = yield EStorageAcquire(name=effect.name, limit=effect.limit)

    if result:
        assert locals_.current_job is not None
        locals_.current_job.acquired.append(effect.name)
    return result


def _handle_release_slot(
    locals_: WorkerLocals, effect: EReleaseSlot
) -> Generator[Any, Any, None]:
    """Release a held concurrency slot immediately and remove it from job-exit cleanup.

    A slot the job does not hold is a no-op — forwarding a release for it would
    wrongly decrement another holder's count.
    """

    job = locals_.current_job
    assert job is not None
    if effect.name not in job.acquired:
        return
    job.acquired.remove(effect.name)
    yield EStorageRelease(name=effect.name)


def make_job_handlers(locals_: WorkerLocals, handler_wrappers: Sequence) -> JobHandlerMap:
    """Create job-effect handlers keyed by effect tag, with any
    user-supplied wrappers applied."""

    bindings = {
        EAcquire.tag: partial(_handle_acquire, locals_),
        EReleaseSlot.tag: partial(_handle_release_slot, locals_),
    }
    return cast(JobHandlerMap, build_handler_map(bindings, handler_wrappers))
