"""Entry point for running a zahir workflow.

Spawns overseer and workers, seeds root job, long-polls for completion.
"""

from collections.abc import Generator, Sequence
from typing import Any, cast

from orbis import handle
from tertius import EEmit, ESpawn, Pid, Scope, SpawnMode, run

from zahir.core.backends.memory import make_memory_storage_handlers
from zahir.core.combinators import merge_handlers
from zahir.core.effects import EEnqueue, EStorageGetError, EStorageGetResult, EStorageIsDone
from zahir.core.evaluate.coordination_handlers import make_coordination_handlers
from zahir.core.evaluate.overseer import run_overseer
from zahir.core.evaluate.runtime import Runtime
from zahir.core.evaluate.worker import worker
from zahir.core.zahir_types import HandlerMap, JobSpec

type EvaluationInputs = tuple[str, tuple, Scope]
type RuntimeBindings = tuple[Sequence, HandlerMap]


def _poll_completion() -> Generator[Any, Any, None]:
    """Long-poll the overseer until all jobs finish, then surface the root result.

    EStorageIsDone parks at the overseer until completion; False means a heartbeat
    or a not-done ack, so the loop simply asks again — no sleep.
    """

    while True:
        done = yield EStorageIsDone()

        if not done:
            continue

        error = yield EStorageGetError()
        if error is not None:
            raise error

        result = yield EStorageGetResult()
        if result is not None:
            yield EEmit(result)
        return


def _kickoff(fn_name: str, args: tuple) -> Generator[Any, Any, None]:
    """Enqueue the root job then poll for completion."""

    yield EEnqueue(job=JobSpec(fn_name=fn_name, args=args))
    yield from _poll_completion()


def _evaluate_runner(
    runtime: Runtime,
    inputs: EvaluationInputs,
    bindings: RuntimeBindings,
) -> Generator[Any, Any, None]:
    """Run the root job and wait for completion."""

    _, _transport, n_workers, n_thread_workers = runtime
    fn_name, args, scope = inputs
    handler_wrappers, handlers = bindings

    overseer: Pid = yield ESpawn(fn_name="run_overseer", args=(handlers,))

    worker_args = (
        bytes(overseer),
        scope,
        handler_wrappers,
        handlers,
    )

    for _ in range(n_workers):
        yield ESpawn(fn_name="worker", args=worker_args, mode=SpawnMode.PROCESS)

    for _ in range(n_thread_workers):
        yield ESpawn(fn_name="worker", args=worker_args, mode=SpawnMode.THREAD)

    # Coordination merged last: the root's transported storage tags must beat the
    # memory backend present in the shared bag, which belongs to the overseer.
    root_handlers = merge_handlers(
        handlers,
        make_coordination_handlers(overseer, handler_wrappers),
    )

    yield from handle(_kickoff(fn_name, args), root_handlers)


def evaluate(  # noqa: PLR0913
    runtime: Runtime,
    fn_name: str,
    args: tuple,
    scope: Scope,
    *,
    handler_wrappers: Sequence = (),
    handlers: HandlerMap | None = None,
) -> Generator[Any, None, None]:
    """Entry point. Run a job and wait for completion."""

    if fn_name not in scope:
        raise KeyError(f"job {fn_name!r} not found in scope")

    memory_handlers = make_memory_storage_handlers(handler_wrappers)
    merged_handlers = cast(HandlerMap, merge_handlers(memory_handlers, handlers or {}))
    full_scope: Scope = {
        "run_overseer": run_overseer,
        "worker": worker,
        **scope,
    }
    _, transport, _n_workers, _n_thread_workers = runtime
    yield from run(
        _evaluate_runner,
        runtime,
        (fn_name, args, scope),
        (handler_wrappers, merged_handlers),
        scope=full_scope,
        transport=transport,
    )
