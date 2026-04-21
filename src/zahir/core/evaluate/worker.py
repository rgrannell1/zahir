"""Worker process: fetches jobs from the overseer, drives them step by step, suspends on EAwait."""

import itertools
from collections.abc import Generator
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from orbis import handle
from tertius import ESelf, ESleep, Pid, Scope

from zahir.core.constants import WORKER_POLL_MS
from zahir.core.effects import (
    EAwait,
    EEnqueue,
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
    _unwrap_reply,
    evaluate_job,
)
from zahir.core.exceptions import JobError, JobTimeout, ZahirException
from zahir.core.scope_proxy import ScopeProxy


@dataclass
class _WaitingJob:
    """A job suspended while waiting for one or more child jobs to complete."""

    fn_name: str  # the name of the function being executed
    eval_gen: Any  # the evaluate_job generator, frozen at yield EAwait
    context: JobHandlerContext
    reply_to: bytes | None  # where to send our result when we finish
    parent_sequence_number: Any  # the sequence_number our parent assigned us
    awaiting: set[int]  # child local sequence_numbers still outstanding
    results: dict[int, Any]  # local sequence_number -> body (result or error)
    result_order: list[int] | None  # None for scalar dispatch; ordered sequence_number list for multi-job dispatch


def _collect_scalar_result(body: Any) -> tuple[Any, Exception | None]:
    """Unwrap a single child result into (value, error)."""

    try:
        return _unwrap_reply(body), None
    except (JobError, JobTimeout) as exc:
        return None, exc


def _collect_multi_results(job: _WaitingJob) -> tuple[list | None, Exception | None]:
    """Collect ordered multi-job results, surfacing the first error if any."""

    first_error: Exception | None = None
    results = []
    for sequence_number in job.result_order:
        try:
            results.append(_unwrap_reply(job.results[sequence_number]))
        except (JobError, JobTimeout) as exc:
            if first_error is None:
                first_error = exc

    if first_error is not None:
        return None, first_error

    return results, None


def _resume_from_result(
    work: tuple,
    waiting: dict[int, _WaitingJob],
    child_to_parent: dict[int, int],
) -> tuple[_WaitingJob, Any, Exception | None] | None:
    """Process a completed child result. Returns (job, handler_value, pending_throw) when all children are done, None if still waiting."""

    _, child_sequence_number, body = work
    parent_key = child_to_parent.pop(child_sequence_number)
    job = waiting[parent_key]
    job.results[child_sequence_number] = body
    job.awaiting.remove(child_sequence_number)

    if job.awaiting:
        return None

    current = waiting.pop(parent_key)

    if current.result_order is not None:
        handler_value, pending_throw = _collect_multi_results(current)
    else:
        handler_value, pending_throw = _collect_scalar_result(body)

    return current, handler_value, pending_throw


def _build_job(work: tuple, ctx: Any) -> _WaitingJob:
    """Construct a WaitingJob from a dequeued job work item."""

    _, fn_name, args, reply_to, timeout_ms, parent_sequence_number = work
    deadline = (
        datetime.now(UTC) + timedelta(milliseconds=timeout_ms) if timeout_ms else None
    )
    job_context = JobHandlerContext(handler_wrappers=ctx.handler_wrappers)
    eval_gen = evaluate_job(ctx._scope[fn_name](ctx, *args), job_context, deadline)

    return _WaitingJob(
        fn_name=fn_name,
        eval_gen=eval_gen,
        context=job_context,
        reply_to=reply_to,
        parent_sequence_number=parent_sequence_number,
        awaiting=set(),
        results={},
        result_order=None,
    )


def _complete_job(job: _WaitingJob, value: Any) -> Generator[Any, Any, None]:
    """Release concurrency slots and report successful completion to the overseer."""

    for name in job.context.acquired:
        yield ERelease(name=name)

    yield EJobComplete(result=value, reply_to=job.reply_to, sequence_number=job.parent_sequence_number, fn_name=job.fn_name)


def _fail_job(job: _WaitingJob, exc: Exception) -> Generator[Any, Any, None]:
    """Release concurrency slots and report job failure to the overseer."""

    for name in job.context.acquired:
        yield ERelease(name=name)
    error = exc if isinstance(exc, ZahirException) else JobError(exc)
    yield EJobFail(error=error, reply_to=job.reply_to, sequence_number=job.parent_sequence_number, fn_name=job.fn_name)


def _enqueue_await(
    effect: EAwait,
    job: _WaitingJob,
    waiting: dict[int, _WaitingJob],
    child_to_parent: dict[int, int],
    alloc: Any,
    me_bytes: bytes,
) -> Generator[Any, Any, None]:
    """Suspend the current job and enqueue all child jobs for the EAwait effect."""

    child_sequence_numbers = [alloc() for _ in effect.jobs]
    parent_key = alloc()

    for cn, spec in zip(child_sequence_numbers, effect.jobs):
        child_to_parent[cn] = parent_key
        yield EEnqueue(
            fn_name=spec.fn_name,
            args=spec.args,
            reply_to=me_bytes,
            timeout_ms=spec.timeout_ms,
            sequence_number=cn,
        )

    waiting[parent_key] = _WaitingJob(
        fn_name=job.fn_name,
        eval_gen=job.eval_gen,
        context=job.context,
        reply_to=job.reply_to,
        parent_sequence_number=job.parent_sequence_number,
        awaiting=set(child_sequence_numbers),
        results={},
        result_order=None if effect.scalar else child_sequence_numbers,
    )


def _worker_body(overseer_pid: Pid, ctx: Any) -> Generator[Any, Any, None]:
    """Worker main loop — drives jobs step by step, suspending onto a local stack on EAwait."""

    me: Pid = yield ESelf()
    me_bytes = bytes(me)

    waiting: dict[int, _WaitingJob] = {}  # parent_key -> _WaitingJob
    child_to_parent: dict[int, int] = {}  # child local sequence_number -> parent_key
    alloc = itertools.count().__next__

    current: _WaitingJob | None = None
    handler_value: Any = None
    pending_throw: Exception | None = None

    while True:
        # ── If no current job, ask the overseer for work ──────────────────────────
        if current is None:
            work = yield EGetJob(worker_pid_bytes=me_bytes)

            if work is None:
                yield ESleep(ms=WORKER_POLL_MS)
                continue

            kind = work[0]

            if kind == "result":
                resumed = _resume_from_result(work, waiting, child_to_parent)
                if resumed is None:
                    continue
                current, handler_value, pending_throw = resumed
                continue

            elif kind == "job":
                _, fn_name, args, reply_to, timeout_ms, parent_sequence_number = work
                if fn_name not in ctx._scope:
                    err = JobError(KeyError(f"job {fn_name!r} not found in scope"))
                    yield EJobFail(error=err, reply_to=reply_to, sequence_number=parent_sequence_number)
                    continue
                current = _build_job(work, ctx)
                handler_value = None
                pending_throw = None

        # ── Advance the current job one step ─────────────────────────────────────
        assert current is not None
        job = current
        try:
            effect = (
                job.eval_gen.throw(pending_throw)
                if pending_throw
                else job.eval_gen.send(handler_value)
            )
            pending_throw = None
        except StopIteration as exc:
            current = None
            yield from _complete_job(job, exc.value)
            handler_value = None
            pending_throw = None
            continue
        except Exception as exc:
            current = None
            yield from _fail_job(job, exc)
            handler_value = None
            pending_throw = None
            continue

        # ── Handle the yielded effect ─────────────────────────────────────────────
        if isinstance(effect, EAwait):
            yield from _enqueue_await(effect, job, waiting, child_to_parent, alloc, me_bytes)
            current = None
            handler_value = None
            pending_throw = None
        else:
            # All other effects (tertius effects, ESleep, etc.) pass through to the runtime
            handler_value = yield effect


def worker(
    overseer_pid_bytes: bytes, scope: Scope, context: type
) -> Generator[Any, Any, None]:
    """zahir worker main loop"""

    overseer = Pid.from_bytes(overseer_pid_bytes)
    ctx = context()
    ctx._scope = scope
    ctx.scope = ScopeProxy(scope)

    coordination_context = CoordinationHandlerContext(overseer=overseer, handler_wrappers=ctx.handler_wrappers)
    yield from handle(
        _worker_body(overseer, ctx),
        **make_coordination_handlers(coordination_context),
    )
