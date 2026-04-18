from collections.abc import Generator
from typing import Any

from tertius import ESleep, ESpawn, Pid, Scope, mcall, run

from constants import COMPLETION_POLL_MS

from evaluate.overseer import run_overseer
from evaluate.worker import worker


def _root(
    fn_name: str,
    args: tuple,
    scope: Scope,
    n_workers: int,
) -> Generator[Any, Any, None]:
    overseer: Pid = yield ESpawn(fn_name="run_overseer", args=(fn_name, args))

    for _ in range(n_workers):
        yield ESpawn(fn_name="worker", args=(bytes(overseer), scope))

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
) -> Generator[Any, None, None]:
    full_scope: Scope = {
        "run_overseer": run_overseer,
        "worker": worker,
        **scope
    }

    yield from run(_root, fn_name, args, scope, n_workers, scope=full_scope)
