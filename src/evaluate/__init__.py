from collections.abc import Generator
from typing import Any

from tertius import ESleep, ESpawn, Pid, Scope, mcall, run

from constants import COMPLETION_POLL_MS, GET_ERROR
from scope_proxy import ScopeProxy

from evaluate.overseer import run_overseer
from evaluate.worker import worker


class JobContext:
    _scope: Scope
    scope: ScopeProxy


def _root(
    fn_name: str,
    args: tuple,
    scope: Scope,
    n_workers: int,
    context: type,
) -> Generator[Any, Any, None]:
    overseer: Pid = yield ESpawn(fn_name="run_overseer", args=(fn_name, args))

    for _ in range(n_workers):
        yield ESpawn(fn_name="worker", args=(bytes(overseer), scope, context))

    while True:
        # check if the overseer is done in a sleep loop
        done = yield from mcall(overseer, "is_done")

        if not done:
            yield ESleep(ms=COMPLETION_POLL_MS)

        # once done, check if there was an error
        error = yield from mcall(overseer, GET_ERROR)

        if error is not None:
            raise error
        return


def evaluate(
    fn_name: str,
    args: tuple,
    scope: Scope,
    n_workers: int = 4,
    context: type = JobContext,
) -> Generator[Any, None, None]:
    # make sure we have the function in scope
    if fn_name not in scope:
        raise KeyError(f"job {fn_name!r} not found in scope")

    # and make sure we could actuall instantiate the context class
    try:
        context()
    except Exception as exc:
        raise TypeError(
            f"context class {context.__name__!r} failed to instantiate: {exc}"
        ) from exc

    # merge the provided scope with the internal functions
    full_scope: Scope = {"run_overseer": run_overseer, "worker": worker, **scope}

    # run the tertius workflow
    yield from run(_root, fn_name, args, scope, n_workers, context, scope=full_scope)
