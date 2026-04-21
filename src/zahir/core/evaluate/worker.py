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
    EAwaitAll,
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

    eval_gen: Any  # the evaluate_job generator, frozen at yield EAwait/EAwaitAll
    context: JobHandlerContext
    reply_to: bytes | None  # where to send our result when we finish
    parent_nonce: Any  # the nonce our parent assigned us
    awaiting: set[int]  # child local nonces still outstanding
    results: dict[int, Any]  # local nonce -> body (result or error)
    result_order: list[int] | None  # None for EAwait; ordered nonce list for EAwaitAll


def _worker_body(overseer_pid: Pid, ctx: Any) -> Generator[Any, Any, None]:
    """Worker main loop — drives jobs step by step, suspending onto a local stack for EAwait/EAwaitAll."""

    me: Pid = yield ESelf()
    me_bytes = bytes(me)

    waiting: dict[int, _WaitingJob] = {}  # parent_key -> _WaitingJob
    child_to_parent: dict[int, int] = {}  # child local nonce -> parent_key
    _alloc = itertools.count().__next__

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
                # A child job completed — find and possibly resume the suspended parent
                _, child_nonce, body = work
                parent_key = child_to_parent.pop(child_nonce)
                job = waiting[parent_key]
                job.results[child_nonce] = body
                job.awaiting.remove(child_nonce)

                if job.awaiting:
                    continue  # still waiting on other children

                # All children done — reconstruct handler_value and resume
                current = waiting.pop(parent_key)
                if current.result_order is not None:
                    # EAwaitAll: ordered list; raise the first error seen
                    first_error: Exception | None = None
                    results = []
                    for n in current.result_order:
                        try:
                            results.append(_unwrap_reply(current.results[n]))
                        except (JobError, JobTimeout) as exc:
                            if first_error is None:
                                first_error = exc
                    if first_error is not None:
                        handler_value = None
                        pending_throw = first_error
                    else:
                        handler_value = results
                        pending_throw = None
                else:
                    # EAwait: single child result
                    try:
                        handler_value = _unwrap_reply(body)
                        pending_throw = None
                    except (JobError, JobTimeout) as exc:
                        handler_value = None
                        pending_throw = exc
                continue

            elif kind == "job":
                # A new job arrived from the queue
                _, fn_name, args, reply_to, timeout_ms, parent_nonce = work

                if fn_name not in ctx._scope:
                    err = JobError(KeyError(f"job {fn_name!r} not found in scope"))
                    yield EJobFail(error=err, reply_to=reply_to, nonce=parent_nonce)
                    continue

                deadline = (
                    datetime.now(UTC) + timedelta(milliseconds=timeout_ms)
                    if timeout_ms
                    else None
                )
                job_context = JobHandlerContext(
                    handler_wrappers=ctx.handler_wrappers,
                )
                eval_gen = evaluate_job(
                    ctx._scope[fn_name](ctx, *args), job_context, deadline
                )
                current = _WaitingJob(
                    eval_gen=eval_gen,
                    context=job_context,
                    reply_to=reply_to,
                    parent_nonce=parent_nonce,
                    awaiting=set(),
                    results={},
                    result_order=None,
                )
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
            for name in job.context.acquired:
                yield ERelease(name=name)
            yield EJobComplete(
                result=exc.value, reply_to=job.reply_to, nonce=job.parent_nonce
            )
            handler_value = None
            pending_throw = None
            continue
        except Exception as exc:
            current = None
            for name in job.context.acquired:
                yield ERelease(name=name)
            error = exc if isinstance(exc, ZahirException) else JobError(exc)
            yield EJobFail(error=error, reply_to=job.reply_to, nonce=job.parent_nonce)
            handler_value = None
            pending_throw = None
            continue

        # ── Handle the yielded effect ─────────────────────────────────────────────
        if isinstance(effect, EAwait):
            # Suspend the current job and enqueue the child
            child_nonce = _alloc()
            parent_key = _alloc()
            child_to_parent[child_nonce] = parent_key
            waiting[parent_key] = _WaitingJob(
                eval_gen=job.eval_gen,
                context=job.context,
                reply_to=job.reply_to,
                parent_nonce=job.parent_nonce,
                awaiting={child_nonce},
                results={},
                result_order=None,
            )
            yield EEnqueue(
                fn_name=effect.fn_name,
                args=effect.args,
                reply_to=me_bytes,
                timeout_ms=effect.timeout_ms,
                nonce=child_nonce,
            )
            current = None
            handler_value = None
            pending_throw = None

        elif isinstance(effect, EAwaitAll):
            # Suspend the current job and enqueue all children
            child_nonces = [_alloc() for _ in effect.effects]
            parent_key = _alloc()
            for cn, eff in zip(child_nonces, effect.effects):
                child_to_parent[cn] = parent_key
                yield EEnqueue(
                    fn_name=eff.fn_name,
                    args=eff.args,
                    reply_to=me_bytes,
                    timeout_ms=eff.timeout_ms,
                    nonce=cn,
                )
            waiting[parent_key] = _WaitingJob(
                eval_gen=job.eval_gen,
                context=job.context,
                reply_to=job.reply_to,
                parent_nonce=job.parent_nonce,
                awaiting=set(child_nonces),
                results={},
                result_order=child_nonces,
            )
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

    coordination_context = CoordinationHandlerContext(overseer=overseer)
    yield from handle(
        _worker_body(overseer, ctx),
        **make_coordination_handlers(coordination_context),
    )
