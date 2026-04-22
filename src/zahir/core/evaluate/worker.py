"""Worker process: fetches jobs from the overseer, drives them step by step, suspends on EAwait."""

from collections.abc import Generator
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from orbis import handle
from tertius import ESelf, ESleep, Pid, Scope

from zahir.core.constants import WORKER_POLL_MS
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
from zahir.core.evaluate.suspension import _RunningJob, _SuspensionTable
from zahir.core.exceptions import JobError, ZahirException
from zahir.core.scope_proxy import ScopeProxy


@dataclass
class _LoopState:
    """Mutable state carried across iterations of the worker loop."""

    current: _RunningJob | None = None
    handler_value: Any = None
    pending_throw: Exception | None = None

    def start(self, job: _RunningJob) -> None:
        """Begin executing a new job."""
        self.current = job
        self.handler_value = None
        self.pending_throw = None

    def reset(self) -> None:
        """Clear state after a job completes, fails, or suspends."""
        self.current = None
        self.handler_value = None
        self.pending_throw = None


def _build_job(work: tuple, ctx: Any) -> _RunningJob:
    """Construct a WaitingJob from a dequeued job work item."""

    _, fn_name, args, reply_to, timeout_ms, parent_sequence_number = work
    deadline = (
        datetime.now(UTC) + timedelta(milliseconds=timeout_ms) if timeout_ms else None
    )
    job_context = JobHandlerContext(handler_wrappers=ctx.handler_wrappers)
    eval_gen = evaluate_job(ctx._scope[fn_name](ctx, *args), job_context, deadline)

    return _RunningJob(
        fn_name=fn_name,
        eval_gen=eval_gen,
        context=job_context,
        reply_to=reply_to,
        parent_sequence_number=parent_sequence_number,
    )


def _complete_job(job: _RunningJob, value: Any) -> Generator[Any, Any, None]:
    """Release concurrency slots and report successful completion to the overseer."""

    for name in job.context.acquired:
        yield ERelease(name=name)
    yield EJobComplete(
        result=value,
        reply_to=job.reply_to,
        sequence_number=job.parent_sequence_number,
        fn_name=job.fn_name,
    )


def _fail_job(job: _RunningJob, exc: Exception) -> Generator[Any, Any, None]:
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


def _worker_body(overseer_pid: Pid, ctx: Any) -> Generator[Any, Any, None]:
    """Worker main loop — drives jobs step by step, suspending onto a local stack on EAwait."""

    me: Pid = yield ESelf()
    me_bytes = bytes(me)

    suspension = _SuspensionTable()
    state = _LoopState()

    while True:
        # ── If no current job, ask the overseer for work ──────────────────────────
        if state.current is None:
            work = yield EGetJob(worker_pid_bytes=me_bytes)

            if work is None:
                yield ESleep(ms=WORKER_POLL_MS)
                continue

            kind = work[0]

            if kind == "result":
                resumed = suspension.resume(work)
                if resumed is None:
                    continue
                state.current, state.handler_value, state.pending_throw = resumed
                continue

            elif kind == "job":
                _, fn_name, args, reply_to, timeout_ms, parent_sequence_number = work
                if fn_name not in ctx._scope:
                    err = JobError(KeyError(f"job {fn_name!r} not found in scope"))
                    yield EJobFail(
                        error=err,
                        reply_to=reply_to,
                        sequence_number=parent_sequence_number,
                    )
                    continue
                state.start(_build_job(work, ctx))

        # ── Advance the current job one step ─────────────────────────────────────
        assert state.current is not None
        job = state.current
        try:
            effect = (
                job.eval_gen.throw(state.pending_throw)
                if state.pending_throw
                else job.eval_gen.send(state.handler_value)
            )
            state.pending_throw = None
        except StopIteration as exc:
            yield from _complete_job(job, exc.value)
            state.reset()
            continue
        except Exception as exc:
            yield from _fail_job(job, exc)
            state.reset()
            continue

        # ── Handle the yielded effect ─────────────────────────────────────────────
        if isinstance(effect, EAwait):
            yield from suspension.suspend(effect, job, me_bytes)
            state.reset()
        else:
            # All other effects (tertius effects, ESleep, etc.) pass through to the runtime
            state.handler_value = yield effect


def worker(
    overseer_pid_bytes: bytes, scope: Scope, context: type
) -> Generator[Any, Any, None]:
    """zahir worker main loop"""

    overseer = Pid.from_bytes(overseer_pid_bytes)
    ctx = context()
    ctx._scope = scope
    ctx.scope = ScopeProxy(scope)

    coordination_context = CoordinationHandlerContext(
        overseer=overseer, handler_wrappers=ctx.handler_wrappers
    )
    yield from handle(
        _worker_body(overseer, ctx),
        **make_coordination_handlers(coordination_context),
    )
