from collections.abc import Generator, Sequence
from typing import Any

from orbis import handle, HandlerDict
from tertius import EEmit, ESleep, ESpawn, Pid, Scope, run

from zahir.core.backends.memory import make_memory_storage_handlers
from zahir.core.constants import COMPLETION_POLL_MS
from zahir.core.effects import EGetError, EGetResult, EIsDone

from zahir.core.evaluate.coordination_handlers import (
    CoordinationHandlerContext,
    make_coordination_handlers,
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
    handlers: HandlerDict,
    storage_handlers: HandlerDict,
) -> Generator[Any, Any, None]:
    overseer: Pid = yield ESpawn(fn_name="run_overseer", args=(fn_name, args, storage_handlers))

    for _ in range(n_workers):
        yield ESpawn(fn_name="worker", args=(bytes(overseer), scope, context, handler_wrappers, handlers))

    ctx = CoordinationHandlerContext(overseer=overseer, handler_wrappers=handler_wrappers)
    root_handlers = {**make_coordination_handlers(ctx), **handlers}
    yield from handle(_poll_completion(), **root_handlers)


def evaluate(
    fn_name: str,
    args: tuple,
    scope: Scope,
    n_workers: int = 4,
    context: type = JobContext,
    handler_wrappers: Sequence = [],
    handlers: HandlerDict = {},
    storage_handlers: HandlerDict = None,
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

    # default to a fresh in-memory storage backend
    resolved_storage = storage_handlers if storage_handlers is not None else make_memory_storage_handlers()

    # merge the provided scope with the internal functions
    full_scope: Scope = {"run_overseer": run_overseer, "worker": worker, **scope}

    # run the tertius workflow
    yield from run(
        _root,
        fn_name, args, scope, n_workers, context,
        handler_wrappers, handlers, resolved_storage,
        scope=full_scope,
    )
