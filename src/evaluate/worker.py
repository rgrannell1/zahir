from collections.abc import Generator
from datetime import UTC, datetime, timedelta
from typing import Any

from orbis import handle
from tertius import Pid, Scope, mcall, mcast

from constants import (
    BLOCKED_EFFECTS,
    RELEASE,
    THROWABLE,
)
from effects import EGetJob, EJobComplete, EJobFail
from evaluate.worker_handlers import make_coordination_handlers, make_handlers
from exceptions import InvalidEffect, JobError, JobTimeout
from scope_proxy import ScopeProxy


def evaluate_job(
    job_gen: Generator[Any, Any, Any],
    overseer: Pid,
    acquired: list[str],
    deadline: datetime | None,
) -> Generator[Any, Any, Any]:
    handlers = make_handlers(overseer, acquired)
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
            if type(effect) in handlers:
                handler_value = yield from handlers[type(effect)](effect)
            elif not hasattr(effect, "tag"):
                raise InvalidEffect(f"job yielded non-Effect value: {effect!r}")
            elif isinstance(effect, BLOCKED_EFFECTS):
                raise InvalidEffect(
                    f"{type(effect).__name__} cannot be yielded directly in a job"
                )
            else:
                handler_value = yield effect
        except THROWABLE as exc:
            pending_throw = exc


def _worker_body(overseer: Pid, ctx: Any) -> Generator[Any, Any, None]:
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
        acquired: list[str] = []

        timed_out = False
        job_error: JobError | None = None
        result: Any = None
        try:
            # try run the job
            if fn_name not in ctx._scope:
                raise KeyError(f"job {fn_name!r} not found in scope")

            result = yield from evaluate_job(
                ctx._scope[fn_name](ctx, *args), overseer, acquired, deadline
            )
        except JobTimeout:
            timed_out = True
        except JobError as exc:
            job_error = exc
        except Exception as exc:
            job_error = JobError(exc)

        # release any semaphores we acquired during the job
        for name in acquired:
            yield from mcast(overseer, (RELEASE, name))

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

    yield from handle(_worker_body(overseer, ctx), **make_coordination_handlers(overseer))
