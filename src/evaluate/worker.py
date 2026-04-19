from collections.abc import Generator
from datetime import UTC, datetime, timedelta
from typing import Any

from orbis import handle
from tertius import Pid, Scope

from constants import (
    BLOCKED_EFFECTS,
    THROWABLE,
)
from effects import EGetJob, EJobComplete, EJobFail, ERelease, ZahirCoordinationEffect
from evaluate.coordination_handlers import CoordinationHandlerContext, make_coordination_handlers
from evaluate.job_handlers import JobHandlerContext, make_job_handlers
from exceptions import InvalidEffect, JobError, JobTimeout
from scope_proxy import ScopeProxy


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
                handler_value = yield effect
        except THROWABLE as exc:
            pending_throw = exc


def _worker_body(
    overseer: Pid, coordination_context: CoordinationHandlerContext, ctx: Any
) -> Generator[Any, Any, None]:
    """Worker main loop — yields coordination effects for job lifecycle."""

    while True:
        # ask for a job, blocking until one is available
        job = yield EGetJob()

        # job acquired
        fn_name, args, reply_to, timeout_ms, nonce = job
        deadline = (
            datetime.now(UTC) + timedelta(milliseconds=timeout_ms)
            if timeout_ms
            else None
        )
        job_context = JobHandlerContext(overseer=overseer)

        timed_out = False
        job_error: JobError | None = None
        result: Any = None
        try:
            # try run the job
            if fn_name not in ctx._scope:
                raise KeyError(f"job {fn_name!r} not found in scope")

            result = yield from evaluate_job(
                ctx._scope[fn_name](ctx, *args), job_context, deadline
            )
        except JobTimeout:
            timed_out = True
        except JobError as exc:
            job_error = exc
        except Exception as exc:
            job_error = JobError(exc)

        # release any concurrency slots acquired during the job
        for name in job_context.acquired:
            yield ERelease(name=name)

        if timed_out:
            # send the timeout error to the caller
            yield EJobFail(error=JobTimeout(), reply_to=reply_to, nonce=nonce)
        elif job_error is not None:
            # send the job error to the caller
            yield EJobFail(error=job_error, reply_to=reply_to, nonce=nonce)
        else:
            # send the result to the overseer waiting for it, and inform the overseer we're done
            yield EJobComplete(result=result, reply_to=reply_to, nonce=nonce)


def worker(
    overseer_pid_bytes: bytes, scope: Scope, context: type
) -> Generator[Any, Any, None]:
    """zahir worker main loop"""

    overseer = Pid.from_bytes(overseer_pid_bytes)
    ctx = context()
    ctx._scope = scope
    ctx.scope = ScopeProxy(scope)

    coordination_context = CoordinationHandlerContext(overseer=overseer)
    yield from handle(
        _worker_body(overseer, coordination_context, ctx),
        **make_coordination_handlers(coordination_context),
    )
