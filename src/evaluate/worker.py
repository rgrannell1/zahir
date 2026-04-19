from collections.abc import Generator
from datetime import UTC, datetime, timedelta
from typing import Any

from tertius import ESend, ESleep, Pid, Scope, mcall, mcast

from constants import GET_JOB, JOB_DONE, RELEASE, WORKER_POLL_MS

from evaluate.worker_handlers import _TIMEOUT_SENTINEL, make_handlers
from exceptions import JobError, JobTimeout

_THROWABLE = (JobTimeout, JobError)


def evaluate_job(
    job_gen: Generator,
    overseer: Pid,
    acquired: list[str],
    deadline: datetime | None,
) -> Generator[Any, Any, Any]:
    handlers = make_handlers(overseer, acquired)
    handler_value: Any = None
    pending_throw: Exception | None = None

    while True:
        try:
            effect = job_gen.throw(pending_throw) if pending_throw else job_gen.send(handler_value)
            pending_throw = None
        except StopIteration as exc:
            return exc.value

        if deadline and datetime.now(UTC) >= deadline:
            pending_throw = JobTimeout()
            continue

        try:
            if type(effect) in handlers:
                handler_value = yield from handlers[type(effect)](effect)
            else:
                handler_value = yield effect
        except _THROWABLE as exc:
            pending_throw = exc


def worker(overseer_pid_bytes: bytes, scope: Scope, context: type) -> Generator[Any, Any, None]:
    overseer = Pid.from_bytes(overseer_pid_bytes)
    ctx = context()
    ctx._scope = scope

    while True:
        job = yield from mcall(overseer, GET_JOB)

        if job is None:
            yield ESleep(ms=WORKER_POLL_MS)
            continue

        fn_name, args, reply_to, timeout_ms = job
        deadline = datetime.now(UTC) + timedelta(milliseconds=timeout_ms) if timeout_ms else None
        acquired: list[str] = []

        timed_out = False
        job_error: JobError | None = None
        try:
            fn_def = ctx._scope[fn_name]
            result = yield from evaluate_job(fn_def(ctx, *args), overseer, acquired, deadline)
        except JobTimeout:
            timed_out = True
        except JobError as exc:
            job_error = exc
        except Exception as exc:
            job_error = JobError(exc)

        for name in acquired:
            yield from mcast(overseer, (RELEASE, name))

        if timed_out:
            if reply_to is not None:
                yield ESend(Pid.from_bytes(reply_to), _TIMEOUT_SENTINEL)
            yield from mcast(overseer, JOB_DONE)
            continue

        if job_error is not None:
            if reply_to is not None:
                yield ESend(Pid.from_bytes(reply_to), job_error)
            yield from mcast(overseer, JOB_DONE)
            continue

        if reply_to is not None:
            yield ESend(Pid.from_bytes(reply_to), result)

        yield from mcast(overseer, JOB_DONE)
