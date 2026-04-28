"""Worker process: fetches jobs from the overseer, drives them step by step, suspends on EAwait."""

from collections.abc import Generator, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from functools import partial, reduce
from typing import Any

from orbis import handle
from tertius import ESelf, ESleep, Pid, Scope

from zahir.core.combinators import apply_wrapper
from zahir.core.constants import WORKER_POLL_MS, WorkItemTag
from zahir.core.effects import (
    EAwait,EAwait through handle() — The _make_suspension_handlers dict and its special-dispatch branch in _handle_running are gone. Instead:
  - _WorkerLocals holds me_bytes and current_job — a mutable cell populated by _worker_body/_handle_running before each step
    EGetJob,
    EJobComplete,
    EJobFail,
    ERelease,
)
from zahir.core.evaluate.coordination_handlers import make_merged_coordination_handlers
from zahir.core.evaluate.job_handlers import (
    JobHandlerContext,
    evaluate_job,
)
from zahir.core.evaluate.suspension import RunningJob, SuspensionTable
from zahir.core.exceptions import JobError, ZahirError
from zahir.core.scope_proxy import ScopeProxy
from zahir.core.zahir_types import HandlerMap, JobContext

# Sentinel returned by the EAwait handler to signal that the job was suspended.
# _handle_running checks for this to transition to _Idle rather than _Running.
_SUSPENDED = object()


@dataclass
class _Idle:
    """Worker is waiting for work from the overseer."""


@dataclass
class _Running:
    """Worker is stepping a job through its generator."""

    job: RunningJob
    handler_value: Any = None
    pending_throw: Exception | None = None


type WorkerState = _Idle | _Running


@dataclass
class _WorkerLocals:
    """Mutable per-worker references written by the worker loop and read by the EAwait handler."""

    me_bytes: bytes = b""
    current_job: RunningJob | None = None


def _build_job(work: tuple, ctx: Any) -> RunningJob:
    """Construct a RunningJob from a dequeued job work item."""

    _, fn_name, args, reply_to, timeout_ms, parent_sequence_number = work
    deadline = datetime.now(UTC) + timedelta(milliseconds=timeout_ms) if timeout_ms else None
    job_context = JobHandlerContext(handler_wrappers=ctx.handler_wrappers)
    eval_gen = evaluate_job(ctx._scope[fn_name](ctx, *args), job_context, deadline)

    return RunningJob(
        fn_name=fn_name,
        eval_gen=eval_gen,
        context=job_context,
        reply_to=reply_to,
        parent_sequence_number=parent_sequence_number,
    )


def _complete_job(job: RunningJob, value: Any) -> Generator[Any, Any, None]:
    """Release concurrency slots and report successful completion to the overseer."""

    for name in job.context.acquired:
        yield ERelease(name=name)
    yield EJobComplete(
        result=value,
        reply_to=job.reply_to,
        sequence_number=job.parent_sequence_number,
        fn_name=job.fn_name,
    )


def _fail_job(job: RunningJob, exc: Exception) -> Generator[Any, Any, None]:
    """Release concurrency slots and report job failure to the overseer."""

    for name in job.context.acquired:
        yield ERelease(name=name)
    error = exc if isinstance(exc, ZahirError) else JobError(exc)
    yield EJobFail(
        error=error,
        reply_to=job.reply_to,
        sequence_number=job.parent_sequence_number,
        fn_name=job.fn_name,
    )


def _handle_idle(
    suspension: SuspensionTable, ctx: Any, me_bytes: bytes
) -> Generator[Any, Any, WorkerState]:
    """Fetch the next work item and transition to the appropriate state."""

    work = yield EGetJob(worker_pid_bytes=me_bytes)

    if work is None:
        yield ESleep(ms=WORKER_POLL_MS)
        return _Idle()

    match work[0]:
        case WorkItemTag.RESULT:
            resumed = suspension.resume(work)
            if resumed is None:
                return _Idle()

            job, handler_value, pending_throw = resumed
            return _Running(job=job, handler_value=handler_value, pending_throw=pending_throw)

        case WorkItemTag.JOB:
            _, fn_name, args, reply_to, timeout_ms, parent_sequence_number = work

            if fn_name not in ctx._scope:
                err = JobError(KeyError(f"job {fn_name!r} not found in scope"))
                yield EJobFail(error=err, reply_to=reply_to, sequence_number=parent_sequence_number)
                return _Idle()
            return _Running(job=_build_job(work, ctx))

        case _:
            return _Idle()


def _handle_eawait(
    suspension: SuspensionTable, locals_: _WorkerLocals, effect: EAwait
) -> Generator[Any, Any, Any]:
    """Suspend the running job and enqueue its child jobs."""

    yield from suspension.suspend(effect, locals_.current_job, locals_.me_bytes)
    return _SUSPENDED


def make_worker_handlers(
    suspension: SuspensionTable, locals_: _WorkerLocals, handler_wrappers: Sequence
) -> HandlerMap:
    """EAwait handler with wrappers applied — merged last so it cannot be overridden."""

    handler = partial(_handle_eawait, suspension, locals_)
    wrapped = reduce(apply_wrapper, handler_wrappers, handler)
    return {EAwait.tag: wrapped}


def _handle_running(
    state: _Running, locals_: _WorkerLocals
) -> Generator[Any, Any, WorkerState]:
    """Advance the current job one step and transition based on the outcome."""

    job = state.job
    locals_.current_job = job
    try:
        effect = job.eval_gen.throw(state.pending_throw) if state.pending_throw else job.eval_gen.send(state.handler_value)
    except StopIteration as exc:
        yield from _complete_job(job, exc.value)
        return _Idle()
    except Exception as exc:  # noqa: BLE001
        yield from _fail_job(job, exc)
        return _Idle()

    handler_value = yield effect
    if handler_value is _SUSPENDED:
        return _Idle()
    return _Running(job=job, handler_value=handler_value, pending_throw=None)


def _worker_body(
    suspension: SuspensionTable, locals_: _WorkerLocals, _overseer_pid: Pid, ctx: Any
) -> Generator[Any, Any, None]:
    """Worker main loop — drives jobs step by step, suspending onto a local stack on EAwait."""

    me: Pid = yield ESelf()
    locals_.me_bytes = bytes(me)
    state: WorkerState = _Idle()

    while True:
        match state:
            case _Idle():
                state = yield from _handle_idle(suspension, ctx, locals_.me_bytes)
            case _Running():
                state = yield from _handle_running(state, locals_)


def worker(
    overseer_pid_bytes: bytes, scope: Scope, user_context, handler_wrappers, handlers: dict
) -> Generator[Any, Any, None]:
    """zahir worker main loop"""

    overseer = Pid.from_bytes(overseer_pid_bytes)
    ctx = JobContext(
        _scope=scope,
        scope=ScopeProxy(scope),
        handler_wrappers=handler_wrappers,
        user_context=user_context() if user_context is not None else None,
    )
    suspension = SuspensionTable()
    locals_ = _WorkerLocals()
    base_handlers = make_merged_coordination_handlers(overseer, handler_wrappers, handlers)
    worker_handlers = make_worker_handlers(suspension, locals_, handler_wrappers)
    yield from handle(
        _worker_body(suspension, locals_, overseer, ctx),
        **base_handlers,
        **worker_handlers,
    )
