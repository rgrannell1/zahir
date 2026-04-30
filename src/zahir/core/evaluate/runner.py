"""Entry point for running a zahir workflow: spawns the overseer and workers, seeds the root job, and polls for completion."""

from collections.abc import Generator, Sequence
from typing import Any

from orbis import HandlerDict, handle
from tertius import EEmit, ESleep, ESpawn, Pid, Scope, run

from zahir.core.backends.memory import make_memory_storage_handlers
from zahir.core.constants import COMPLETION_POLL_MS
from zahir.core.effects import EEnqueue, EGetError, EGetResult, EIsDone
from zahir.core.evaluate.coordination_handlers import make_merged_coordination_handlers
from zahir.core.evaluate.overseer import run_overseer
from zahir.core.evaluate.worker import worker


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


def _kickoff(fn_name: str, args: tuple) -> Generator[Any, Any, None]:
    """Enqueue the root job then poll for completion."""

    yield EEnqueue(fn_name=fn_name, args=args, reply_to=None, timeout_ms=None, sequence_number=None)
    yield from _poll_completion()


def _evaluate_runner(
    fn_name: str,
    args: tuple,
    scope: Scope,
    n_workers: int,
    handler_wrappers: Sequence,
    handlers: HandlerDict,
) -> Generator[Any, Any, None]:
    """Run the root job and wait for completion."""

    overseer: Pid = yield ESpawn(fn_name="run_overseer", args=(handlers,))

    for _ in range(n_workers):
        yield ESpawn(fn_name="worker", args=(bytes(overseer), scope, handler_wrappers, handlers))

    root_handlers = make_merged_coordination_handlers(overseer, handler_wrappers, handlers)

    yield from handle(_kickoff(fn_name, args), **root_handlers)


def evaluate(
    fn_name: str,
    args: tuple,
    scope: Scope,
    *,
    n_workers: int = 4,
    handler_wrappers: Sequence = (),
    handlers: HandlerDict | None = None,
) -> Generator[Any, None, None]:
    """Entry point. Run a job and wait for completion."""

    if fn_name not in scope:
        raise KeyError(f"job {fn_name!r} not found in scope")

    merged_handlers = {**make_memory_storage_handlers(), **(handlers or {})}
    full_scope: Scope = {"run_overseer": run_overseer, "worker": worker, **scope}

    yield from run(
        _evaluate_runner,
        fn_name,
        args,
        scope,
        n_workers,
        handler_wrappers,
        merged_handlers,
        scope=full_scope,
    )
