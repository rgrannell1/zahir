"""Worker process: fetches jobs from the overseer, drives them step by step, suspends on EAwait."""

from collections.abc import Generator
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from orbis import handle
from tertius import ESelf, ESleep, Pid, Scope

from zahir.core.constants import WORKER_POLL_MS, WorkItemTag
from zahir.core.effects import (
    EAwait,
    EGetJob,
    EJobComplete,
    EJobFail,
    ERelease,
)
from zahir.core.evaluate.coordination_handlers import (
    CoordinationHandlerContext,
    make_coordination_handlers,
)
from zahir.core.evaluate.job_handlers import (
    JobHandlerContext,
    evaluate_job,
)
from zahir.core.evaluate.suspension import RunningJob, SuspensionTable
from zahir.core.exceptions import JobError, ZahirException
from zahir.core.scope_proxy import ScopeProxy


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


def _build_job(work: tuple, ctx: Any) -> RunningJob:
    """Construct a RunningJob from a dequeued job work item."""

    _, fn_name, args, reply_to, timeout_ms, parent_sequence_number = work
    if timeout_ms:
        deadline = datetime.now(UTC) + timedelta(milliseconds=timeout_ms)
    else:
        deadline = None
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
    error = exc if isinstance(exc, ZahirException) else JobError(exc)
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


def _handle_running(
    state: _Running, suspension: SuspensionTable, me_bytes: bytes
) -> Generator[Any, Any, WorkerState]:
    """Advance the current job one step and transition based on the outcome."""

    job = state.job
    try:
        if state.pending_throw:
            effect = job.eval_gen.throw(state.pending_throw)
        else:
            effect = job.eval_gen.send(state.handler_value)
    except StopIteration as exc:
        yield from _complete_job(job, exc.value)
        return _Idle()
    except Exception as exc:
        yield from _fail_job(job, exc)
        return _Idle()

    if isinstance(effect, EAwait):
        yield from suspension.suspend(effect, job, me_bytes)
        return _Idle()

    handler_value = yield effect
    return _Running(job=job, handler_value=handler_value, pending_throw=None)


def _worker_body(overseer_pid: Pid, ctx: Any) -> Generator[Any, Any, None]:
    """Worker main loop — drives jobs step by step, suspending onto a local stack on EAwait."""

    me: Pid = yield ESelf()
    me_bytes = bytes(me)

    suspension = SuspensionTable()
    state: WorkerState = _Idle()

    while True:
        match state:
            case _Idle():
                state = yield from _handle_idle(suspension, ctx, me_bytes)
            case _Running():
                state = yield from _handle_running(state, suspension, me_bytes)


def worker(
    overseer_pid_bytes: bytes, scope: Scope, context: type, handler_wrappers
) -> Generator[Any, Any, None]:
    """zahir worker main loop"""

    overseer = Pid.from_bytes(overseer_pid_bytes)
    ctx = context()
    ctx._scope = scope
    ctx.scope = ScopeProxy(scope)
    ctx.handler_wrappers = handler_wrappers

    coordination_context = CoordinationHandlerContext(
        overseer=overseer, handler_wrappers=handler_wrappers
    )
    yield from handle(
        _worker_body(overseer, ctx),
        **make_coordination_handlers(coordination_context),
    )
