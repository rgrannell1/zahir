from collections.abc import Generator
from typing import Any

from tertius import ESleep, ESpawn, Pid, Scope, mcall, run

from constants import COMPLETION_POLL_MS

from evaluate.overseer import run_overseer
from evaluate.worker import worker


class JobContext:
    pass


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
        done = yield from mcall(overseer, "is_done")
        if done:
            return
        yield ESleep(ms=COMPLETION_POLL_MS)


def evaluate(
    fn_name: str,
    args: tuple,
    scope: Scope,
    n_workers: int = 4,
    context: type = JobContext,
) -> Generator[Any, None, None]:
    full_scope: Scope = {
        "run_overseer": run_overseer,
        "worker": worker,
        **scope
    }

    yield from run(_root, fn_name, args, scope, n_workers, context, scope=full_scope)
