"""Worker process: fetches jobs from the overseer, drives them step by step, suspends on EAwait."""

import time
from collections.abc import Generator, Sequence
from dataclasses import dataclass
from functools import partial
from typing import Any

from orbis import handle
from tertius import EEmit, ESelf, Pid, Scope

from zahir.core.clock import monotonic_deadline
from zahir.core.combinators import build_handler_map, merge_handlers
from zahir.core.effects import (
    EAwait,
    EGetJob,
    EJobComplete,
    EJobFail,
    EStorageRelease,
)
from zahir.core.emit import execute_start_event, format_job_id
from zahir.core.evaluate.coordination_handlers import make_coordination_handlers
from zahir.core.evaluate.job_handlers import (
    evaluate_job,
    make_job_handlers,
)
from zahir.core.evaluate.suspension import RunningJob, SuspensionTable, WorkerLocals
from zahir.core.exceptions import JobError, JobTimeoutError, ZahirError
from zahir.core.fp_types import Err, Ok
from zahir.core.scope_proxy import ScopeProxy
from zahir.core.telemetry import record_execute_start
from zahir.core.zahir_types import HandlerMap, JobContext, JobSpec, ResultItem

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
    job_call: Generator = ctx._scope[spec.fn_name](ctx, *spec.args, **spec.kwargs)
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
    if spec.fn_name not in ctx._scope:
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
        yield from ()  # unreachable: required to make this function a generator
        return []
    yield from suspension.suspend(effect, locals_.current_job, locals_.me_bytes)
    return _SUSPENDED


def make_worker_handlers(
    suspension: SuspensionTable, locals_: WorkerLocals, handler_wrappers: Sequence
) -> HandlerMap:
    """EAwait handler with wrappers applied — merged last so it cannot be overridden."""

    bindings = {EAwait.tag: partial(_handle_eawait, suspension, locals_)}
    return build_handler_map(bindings, handler_wrappers)


def advance_job(
    job: RunningJob, pending_throw: Exception | None, handler_value: Any
) -> Any:
    """Drive job generator one step, throwing exception if pending."""
    if pending_throw:
        return job.eval_gen.throw(pending_throw)
    return job.eval_gen.send(handler_value)


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
        effect = advance_job(job, state.pending_throw, state.handler_value)
    except StopIteration as exc:
        yield from _guarded_success(job, exc.value)
        return _Idle()
    except Exception as exc:  # noqa: BLE001
        yield from _failed_job(job, exc)
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


def worker(  # noqa: PLR0913
    overseer_pid_bytes: bytes,
    scope: Scope,
    handler_wrappers,
    handlers: HandlerMap,
    *,
    max_silence_ms: int | None = None,
) -> Generator[Any, Any, None]:
    """zahir worker main loop"""

    overseer = Pid.from_bytes(overseer_pid_bytes)
    ctx: JobContext = JobContext(
        _scope=scope,
        scope=ScopeProxy(scope),
    )

    suspension = SuspensionTable()
    locals_ = WorkerLocals()

    # acquire & similar
    job_handlers = make_job_handlers(locals_, handler_wrappers)
    # general coordination handlers — merged last so transported storage tags beat
    # any storage handlers present in the shared bag (they belong to the overseer)
    coordination = make_coordination_handlers(overseer, handler_wrappers, max_silence_ms)
    base_handlers = merge_handlers(handlers, coordination)
    # eawait handler, which uses locals_ for local state
    worker_handlers = make_worker_handlers(suspension, locals_, handler_wrappers)

    yield from handle(
        _worker_body(suspension, locals_, job_handlers, overseer, ctx),
        merge_handlers(base_handlers, worker_handlers),
    )
