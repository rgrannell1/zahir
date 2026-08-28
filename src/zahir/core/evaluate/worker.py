"""Worker process: fetches jobs from the overseer, drives them step by step, suspends on EAwait."""

import time
from collections.abc import Generator, Sequence
from dataclasses import dataclass
from functools import partial
from typing import Any

from orbis import handle
from tertius import EEmit, ESelf, Pid, Scope

from zahir.core.combinators import build_handler_map, merge_handlers
from zahir.core.commons.clock import monotonic_deadline
from zahir.core.commons.fp_types import Err, Ok
from zahir.core.commons.generators import resume
from zahir.core.commons.zahir_types import HandlerMap, JobContext, JobSpec, ResultItem
from zahir.core.effects import (
    EAwait,
    EGetJob,
    EJobComplete,
    EJobFail,
    EStorageRelease,
)
from zahir.core.evaluate.coordination_handlers import (
    make_coordination_handlers,
    require_storage_transported,
)
from zahir.core.evaluate.job_handlers import (
    evaluate_job,
    make_job_handlers,
)
from zahir.core.evaluate.suspension import RunningJob, SuspensionTable, WorkerLocals
from zahir.core.evaluate.worker_types import WorkerHandlerOptions
from zahir.core.exceptions import JobError, JobTimeoutError, ZahirError
from zahir.core.scope_proxy import ScopeProxy
from zahir.core.telemetry import execute_start_event, format_job_id, record_execute_start

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


def _build_job(spec: JobSpec, ctx: Any, job_handlers: HandlerMap) -> RunningJob:
    """Construct a RunningJob from a dequeued JobSpec."""

    deadline = monotonic_deadline(spec.timeout_ms)
    job_call: Generator = ctx.fns[spec.fn_name](ctx, *spec.args, **spec.kwargs)
    eval_gen: Generator = evaluate_job(job_call, job_handlers, deadline)

    return RunningJob(
        fn_name=spec.fn_name,
        eval_gen=eval_gen,
        reply_to=spec.reply_to,
        parent_sequence_number=spec.sequence_number,
        deadline=deadline,
    )


def _successful_job(job: RunningJob, value: Any) -> Generator[Any, Any, None]:
    """Release concurrency slots and report successful completion to the overseer."""

    # Release concurrency slots; we're done
    for name in job.acquired:
        yield EStorageRelease(name=name)

    # Report successful completion to the overseer
    yield EJobComplete(
        result=value,
        reply_to=job.reply_to,
        sequence_number=job.parent_sequence_number,
        fn_name=job.fn_name,
    )


def _failed_job(job: RunningJob, exc: Exception) -> Generator[Any, Any, None]:
    """Release concurrency slots and report job failure to the overseer."""

    for name in job.acquired:
        yield EStorageRelease(name=name)

    error = exc if isinstance(exc, ZahirError) else JobError(exc)
    yield EJobFail(
        error=error,
        reply_to=job.reply_to,
        sequence_number=job.parent_sequence_number,
        fn_name=job.fn_name,
    )


def _handle_result_work_item(suspension: SuspensionTable, work) -> WorkerState:
    """Resume a suspended job from a buffered result."""
    resumed = suspension.resume(work)
    if resumed is None:
        return _Idle()
    job, result = resumed
    match result:
        case Ok(value):
            return _Running(job=job, handler_value=value)
        case Err(error):
            return _Running(job=job, pending_throw=error)


def _handle_job_work_item(
    spec: JobSpec, ctx: Any, job_handlers: HandlerMap
) -> Generator[Any, Any, WorkerState]:
    """Validate scope membership and build a RunningJob from a dequeued JobSpec."""
    if spec.fn_name not in ctx.fns:
        err = JobError(KeyError(f"job {spec.fn_name!r} not found in scope"))
        yield EJobFail(error=err, reply_to=spec.reply_to, sequence_number=spec.sequence_number)
        return _Idle()
    if isinstance(spec.reply_to, bytes) and spec.sequence_number is not None:
        job_id = format_job_id(spec.reply_to, spec.sequence_number)
    else:
        job_id = "root"
    record_execute_start(job_id)
    yield EEmit(execute_start_event(spec.fn_name, job_id))
    return _Running(job=_build_job(spec, ctx, job_handlers))


def _expire_or_idle(suspension: SuspensionTable) -> WorkerState:
    """Fail one expired suspended parent by resuming it with JobTimeoutError, or stay idle."""

    expired = suspension.pop_expired(time.monotonic())
    if expired is None:
        return _Idle()
    return _Running(job=expired, pending_throw=JobTimeoutError())


def _handle_idle(
    suspension: SuspensionTable, ctx: Any, me_bytes: bytes, job_handlers: HandlerMap
) -> Generator[Any, Any, WorkerState]:
    """Fetch the next work item and transition to the appropriate state.

    EGetJob long-polls: it blocks at the overseer until work or a wake arrives, so a
    None (heartbeat or wake ack) loops straight back into a fresh request — no sleep.
    Empty-handed passes sweep the suspension table, so a suspended parent's timeout
    fires within one park window rather than never.
    """

    work = yield EGetJob(worker_pid_bytes=me_bytes)

    match work:
        case None:
            return _expire_or_idle(suspension)
        case ResultItem():
            return _handle_result_work_item(suspension, work)
        case JobSpec():
            return (yield from _handle_job_work_item(work, ctx, job_handlers))
        case _:
            return _Idle()


def _handle_eawait(
    suspension: SuspensionTable, locals_: WorkerLocals, effect: EAwait
) -> Generator[Any, Any, Any]:
    """Suspend the running job and enqueue its child jobs."""

    assert locals_.current_job is not None
    if not effect.jobs:
        # Empty fan-out: nothing to enqueue, so no child can ever complete and resume the parent.
        # Return immediately with an empty result list rather than suspending forever.
        return []
    yield from suspension.suspend(effect, locals_.current_job, locals_.me_bytes)
    return _SUSPENDED


def make_worker_handlers(
    suspension: SuspensionTable, locals_: WorkerLocals, handler_wrappers: Sequence
) -> HandlerMap:
    """EAwait handler with wrappers applied — merged last so it cannot be overridden."""

    bindings = {EAwait.tag: partial(_handle_eawait, suspension, locals_)}
    return build_handler_map(bindings, handler_wrappers)


def _guarded_success(job: RunningJob, value: Any) -> Generator[Any, Any, None]:
    """Report success; if the report itself fails (e.g. unpicklable result), report failure.

    A failure while reporting the failure propagates — the worker cannot do better,
    and dying loudly beats losing the job silently.
    """

    try:
        yield from _successful_job(job, value)
    except Exception as exc:  # noqa: BLE001
        yield from _failed_job(job, exc)


def _handle_running(state: _Running, locals_: WorkerLocals) -> Generator[Any, Any, WorkerState]:
    """Advance the current job one step and transition based on the outcome.

    A worker-level handler failure during the yielded effect is thrown back into
    the job — the job may catch it; otherwise it fails and is reported normally.
    """

    job = state.job
    locals_.current_job = job
    try:
        effect, done, return_value = resume(job.eval_gen, state.handler_value, state.pending_throw)
    except Exception as exc:  # noqa: BLE001
        yield from _failed_job(job, exc)
        return _Idle()
    if done:
        yield from _guarded_success(job, return_value)
        return _Idle()

    try:
        handler_value = yield effect
    except Exception as exc:  # noqa: BLE001
        return _Running(job=job, pending_throw=exc)

    if handler_value is _SUSPENDED:
        return _Idle()
    return _Running(job=job, handler_value=handler_value, pending_throw=None)


def _worker_body(
    suspension: SuspensionTable,
    locals_: WorkerLocals,
    job_handlers: HandlerMap,
    _overseer_pid: Pid,
    ctx: Any,
) -> Generator[Any, Any, None]:
    """Worker main loop — drives jobs step by step, suspending onto a local stack on EAwait."""

    me: Pid = yield ESelf()
    locals_.me_bytes = bytes(me)
    state: WorkerState = _Idle()

    while True:
        match state:
            case _Idle():
                state = yield from _handle_idle(suspension, ctx, locals_.me_bytes, job_handlers)
            case _Running():
                state = yield from _handle_running(state, locals_)


def build_worker_handler_maps(options: WorkerHandlerOptions) -> tuple[HandlerMap, HandlerMap]:
    """Build job and effect handler maps for one worker."""

    job_handlers = make_job_handlers(options.worker_locals, options.handler_wrappers)
    coordination = make_coordination_handlers(
        options.overseer,
        options.handler_wrappers,
        options.max_silence_ms,
    )
    user_handlers = build_handler_map(options.handlers, options.handler_wrappers)
    base_handlers = merge_handlers(user_handlers, coordination)
    require_storage_transported(base_handlers, coordination)
    suspension_handlers = make_worker_handlers(
        options.suspension,
        options.worker_locals,
        options.handler_wrappers,
    )
    return job_handlers, merge_handlers(base_handlers, suspension_handlers)


def worker(  # noqa: PLR0913
    overseer_pid_bytes: bytes,
    scope: Scope,
    handler_wrappers: Sequence,
    handlers: HandlerMap,
    *,
    max_silence_ms: int | None = None,
) -> Generator[Any, Any, None]:
    """zahir worker main loop"""

    overseer = Pid.from_bytes(overseer_pid_bytes)
    ctx: JobContext = JobContext(
        fns=scope,
        scope=ScopeProxy(scope),
    )

    suspension = SuspensionTable()
    locals_ = WorkerLocals()
    options = WorkerHandlerOptions(
        suspension=suspension,
        worker_locals=locals_,
        overseer=overseer,
        handler_wrappers=handler_wrappers,
        handlers=handlers,
        max_silence_ms=max_silence_ms,
    )
    job_handlers, worker_handlers = build_worker_handler_maps(options)
    worker_body = _worker_body(suspension, locals_, job_handlers, overseer, ctx)
    yield from handle(worker_body, worker_handlers)
