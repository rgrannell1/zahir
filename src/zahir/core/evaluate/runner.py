"""Entry point for running a zahir workflow.

Spawns overseer and workers, seeds root job, long-polls for completion.
"""

from collections.abc import Generator, Sequence
from typing import Any

from orbis import handle
from tertius import EEmit, ESpawn, Pid, Scope, SpawnMode, run

from zahir.core.backends.memory import make_memory_storage_handlers
from zahir.core.combinators import build_handler_map, merge_handlers
from zahir.core.commons.fp_types import Ok
from zahir.core.commons.zahir_types import HandlerMap, JobSpec, RootResult
from zahir.core.effects import (
    STORAGE_TAGS,
    EEnqueue,
    EStorageGetError,
    EStorageGetResult,
    EStorageIsDone,
)
from zahir.core.evaluate.coordination_handlers import (
    make_coordination_handlers,
    require_storage_transported,
)
from zahir.core.evaluate.overseer import run_overseer
from zahir.core.evaluate.runtime import Runtime
from zahir.core.evaluate.worker import worker
from zahir.core.exceptions import ZahirError

type EvaluationInputs = tuple[str, tuple, Scope]

# (handler_wrappers, overseer bag: storage+user, worker/root bag: user only)
type RuntimeBindings = tuple[Sequence, HandlerMap, HandlerMap]


def _poll_completion() -> Generator[Any, Any, None]:
    """Long-poll the overseer until all jobs finish, then surface the root result.

    EStorageIsDone parks at the overseer until completion; False means a heartbeat
    or a not-done ack, so the loop simply asks again — no sleep. The root result
    arrives as an Ok and is emitted wrapped in RootResult, so consumers can pick
    it out of the stream — including a root job that returned None.
    """

    while True:
        done = yield EStorageIsDone()

        if not done:
            continue

        error = yield EStorageGetError()
        if error is not None:
            raise error

        result = yield EStorageGetResult()
        match result:
            case Ok(value):
                yield EEmit(RootResult(value))
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

    fn_name, args, scope = inputs
    handler_wrappers, overseer_handlers, handlers = bindings

    # Only the overseer holds the storage backend; workers and the root carry
    # user handlers plus transport bindings.
    overseer: Pid = yield ESpawn(fn_name="run_overseer", args=(overseer_handlers,))

    worker_args = (
        bytes(overseer),
        scope,
        handler_wrappers,
        handlers,
    )

    for _ in range(runtime.n_workers):
        yield ESpawn(fn_name="worker", args=worker_args, mode=SpawnMode.PROCESS)

    for _ in range(runtime.n_thread_workers):
        yield ESpawn(fn_name="worker", args=worker_args, mode=SpawnMode.THREAD)

    # Coordination merged last: transported storage tags must beat any storage
    # handlers a user-supplied bag might contain.
    coordination = make_coordination_handlers(overseer, handler_wrappers)
    root_handlers = merge_handlers(build_handler_map(handlers, handler_wrappers), coordination)
    require_storage_transported(root_handlers, coordination)

    yield from handle(_kickoff(fn_name, args), root_handlers)


def evaluate(  # noqa: PLR0913
    runtime: Runtime,
    fn_name: str,
    args: tuple,
    scope: Scope,
    *,
    handler_wrappers: Sequence = (),
    handlers: HandlerMap | None = None,
) -> Generator[Any]:
    """Entry point. Run a job and wait for completion."""

    if fn_name not in scope:
        raise KeyError(f"job {fn_name!r} not found in scope")

    user_storage_tags = [tag for tag in handlers or {} if tag in STORAGE_TAGS]
    if user_storage_tags:
        raise ZahirError(f"user handlers may not bind storage tags: {user_storage_tags}")

    memory_handlers = make_memory_storage_handlers(handler_wrappers)
    overseer_handlers = merge_handlers(
        memory_handlers, build_handler_map(handlers or {}, handler_wrappers)
    )
    full_scope: Scope = {
        "run_overseer": run_overseer,
        "worker": worker,
        **scope,
    }
    yield from run(
        _evaluate_runner,
        runtime,
        (fn_name, args, scope),
        (handler_wrappers, overseer_handlers, handlers or {}),
        scope=full_scope,
        transport=runtime.transport,
    )
