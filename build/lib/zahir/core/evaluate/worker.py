from collections.abc import Generator
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any

from orbis import handle
from tertius import ESelf, ESleep, Pid, Scope, mcall, mcast

from zahir.core.constants import (
    ENQUEUE,
    GET_JOB,
    JOB_DONE,
    JOB_FAILED,
    RELEASE,
    WORKER_POLL_MS,
)
from zahir.core.effects import EAwait, EAwaitAll
from zahir.core.evaluate.job_handlers import (
    JobHandlerContext,
    _unwrap_reply,
    evaluate_job,
)
from zahir.core.exceptions import JobError, JobTimeout
from zahir.core.scope_proxy import ScopeProxy


@dataclass
class _WaitingJob:
    """A job suspended while waiting for one or more child jobs to complete."""

    eval_gen: Any  # the evaluate_job generator, frozen at yield EAwait/EAwaitAll
    context: JobHandlerContext
    reply_to: bytes | None  # where to send our result when we finish
    parent_sequence_number: Any  # the sequence_number our parent assigned us
    awaiting: set[int]  # child local sequence_numbers still outstanding
    results: dict[int, Any]  # local sequence_number -> body (result or error)
    result_order: list[int] | None  # None for EAwait; ordered sequence_number list for EAwaitAll


def _worker_body(overseer_pid: Pid, ctx: Any) -> Generator[Any, Any, None]:
    """Worker main loop — drives jobs step by step, suspending onto a local stack for EAwait/EAwaitAll."""

    me: Pid = yield ESelf()
    me_bytes = bytes(me)

    waiting: dict[int, _WaitingJob] = {}  # parent_key -> _WaitingJob
    child_to_parent: dict[int, int] = {}  # child local sequence_number -> parent_key
    _counter = [0]

    def _alloc() -> int:
        n = _counter[0]
        _counter[0] += 1
        return n

    current: _WaitingJob | None = None
    handler_value: Any = None
    pending_throw: Exception | None = None

    while True:
        # ── If no current job, ask the overseer for work ──────────────────────────
        if current is None:
            work = yield from mcall(overseer_pid, (GET_JOB, me_bytes))

            if work is None:
                yield ESleep(ms=WORKER_POLL_MS)
                continue

            kind = work[0]

            if kind == "result":
                # A child job completed — find and possibly resume the suspended parent
                _, child_sequence_number, body = work
                parent_key = child_to_parent.pop(child_sequence_number)
                job = waiting[parent_key]
                job.results[child_sequence_number] = body
                job.awaiting.discard(child_sequence_number)

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
                _, fn_name, args, reply_to, timeout_ms, parent_sequence_number = work

                if fn_name not in ctx._scope:
                    err = JobError(KeyError(f"job {fn_name!r} not found in scope"))
                    yield from mcast(
                        overseer_pid,
                        (JOB_DONE, reply_to, parent_sequence_number, err)
                        if reply_to
                        else (JOB_FAILED, err),
                    )
                    continue

                deadline = (
                    datetime.now(UTC) + timedelta(milliseconds=timeout_ms)
                    if timeout_ms
                    else None
                )
                job_context = JobHandlerContext(
                    overseer=overseer_pid,
                    scope=ctx._scope,
                    job_ctx=ctx,
                    handler_wrappers=ctx.handler_wrappers,
                )
                eval_gen = evaluate_job(
                    ctx._scope[fn_name](ctx, *args), job_context, deadline
                )
                current = _WaitingJob(
                    eval_gen=eval_gen,
                    context=job_context,
                    reply_to=reply_to,
                    parent_sequence_number=parent_sequence_number,
                    awaiting=set(),
                    results={},
                    result_order=None,
                )
                handler_value = None
                pending_throw = None

        # ── Advance the current job one step ─────────────────────────────────────
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
                yield from mcast(overseer_pid, (RELEASE, name))
            yield from mcast(
                overseer_pid, (JOB_DONE, job.reply_to, job.parent_sequence_number, exc.value)
            )
            handler_value = None
            pending_throw = None
            continue
        except (JobError, JobTimeout) as exc:
            current = None
            for name in job.context.acquired:
                yield from mcast(overseer_pid, (RELEASE, name))
            if job.reply_to is not None:
                yield from mcast(
                    overseer_pid, (JOB_DONE, job.reply_to, job.parent_sequence_number, exc)
                )
            else:
                yield from mcast(overseer_pid, (JOB_FAILED, exc))
            handler_value = None
            pending_throw = None
            continue
        except Exception as exc:
            current = None
            for name in job.context.acquired:
                yield from mcast(overseer_pid, (RELEASE, name))
            wrapped = JobError(exc)
            if job.reply_to is not None:
                yield from mcast(
                    overseer_pid, (JOB_DONE, job.reply_to, job.parent_sequence_number, wrapped)
                )
            else:
                yield from mcast(overseer_pid, (JOB_FAILED, wrapped))
            handler_value = None
            pending_throw = None
            continue

        # ── Handle the yielded effect ─────────────────────────────────────────────
        if isinstance(effect, EAwait):
            # Suspend the current job and enqueue the child
            child_sequence_number = _alloc()
            parent_key = _alloc()
            child_to_parent[child_sequence_number] = parent_key
            waiting[parent_key] = _WaitingJob(
                eval_gen=job.eval_gen,
                context=job.context,
                reply_to=job.reply_to,
                parent_sequence_number=job.parent_sequence_number,
                awaiting={child_sequence_number},
                results={},
                result_order=None,
            )
            yield from mcast(
                overseer_pid,
                (
                    ENQUEUE,
                    effect.fn_name,
                    effect.args,
                    me_bytes,
                    effect.timeout_ms,
                    child_sequence_number,
                ),
            )
            current = None
            handler_value = None
            pending_throw = None

        elif isinstance(effect, EAwaitAll):
            # Suspend the current job and enqueue all children
            child_sequence_numbers = [_alloc() for _ in effect.effects]
            parent_key = _alloc()
            for cn, eff in zip(child_sequence_numbers, effect.effects):
                child_to_parent[cn] = parent_key
                yield from mcast(
                    overseer_pid,
                    (ENQUEUE, eff.fn_name, eff.args, me_bytes, eff.timeout_ms, cn),
                )
            waiting[parent_key] = _WaitingJob(
                eval_gen=job.eval_gen,
                context=job.context,
                reply_to=job.reply_to,
                parent_sequence_number=job.parent_sequence_number,
                awaiting=set(child_sequence_numbers),
                results={},
                result_order=child_sequence_numbers,
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

    yield from handle(_worker_body(overseer, ctx))
