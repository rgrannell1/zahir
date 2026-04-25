from collections.abc import Generator, Sequence
from typing import Any

from orbis import handle
from tertius import EEmit, ESleep, ESpawn, Pid, Scope, run

from zahir.core.constants import COMPLETION_POLL_MS
from zahir.core.effects import EGetError, EGetResult, EIsDone

from zahir.core.evaluate.coordination_handlers import (
    CoordinationHandlerContext,
    make_root_handlers,
)
from zahir.core.evaluate.overseer import run_overseer
from zahir.core.evaluate.worker import worker


class JobContext:
    """Context object passed as the first argument to every job function."""


def _poll_completion() -> Generator[Any, Any, None]:
    """Poll the overseer until all jobs finish, then surface the root result."""

    while True:
        done = yield EIsDone()
        if not done:
            yield ESleep(ms=COMPLETION_POLL_MS)
            continue

        error = yield EGetError()
        if error is not None:
            raise error

        result = yield EGetResult()
        if result is not None:
            yield EEmit(result)
        return


def _root(
    fn_name: str,
    args: tuple,
    scope: Scope,
    n_workers: int,
    context: type,
    handler_wrappers: Sequence,
) -> Generator[Any, Any, None]:
    overseer: Pid = yield ESpawn(fn_name="run_overseer", args=(fn_name, args, handler_wrappers))

    for _ in range(n_workers):
        yield ESpawn(fn_name="worker", args=(bytes(overseer), scope, context, handler_wrappers))

    ctx = CoordinationHandlerContext(overseer=overseer, handler_wrappers=handler_wrappers)
    yield from handle(_poll_completion(), **make_root_handlers(ctx))


def evaluate(
    fn_name: str,
    args: tuple,
    scope: Scope,
    n_workers: int = 4,
    context: type = JobContext,
    handler_wrappers: Sequence = [],
) -> Generator[Any, None, None]:
    # make sure we have the function in scope
    if fn_name not in scope:
        raise KeyError(f"job {fn_name!r} not found in scope")

    # and make sure we could actually instantiate the context class
    try:
        context()
    except Exception as exc:
        raise TypeError(
            f"context class {context.__name__!r} failed to instantiate: {exc}"
        ) from exc

    # merge the provided scope with the internal functions
    full_scope: Scope = {"run_overseer": run_overseer, "worker": worker, **scope}

    # run the tertius workflow
    yield from run(_root, fn_name, args, scope, n_workers, context, handler_wrappers, scope=full_scope)
