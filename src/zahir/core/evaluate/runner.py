"""Entry point for running a zahir workflow.

Spawns overseer and workers, seeds root job, long-polls for completion.
"""

from collections.abc import Generator, Sequence
from typing import Any

from orbis import BindingMap, handle
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
from zahir.core.evaluate.worker_types import InterpreterWrappers
from zahir.core.exceptions import ZahirError

type EvaluationInputs = tuple[str, tuple, Scope]

# Wrappers, overseer handlers, worker handlers, and worker providers.
type RuntimeBindings = tuple[InterpreterWrappers, HandlerMap, HandlerMap, BindingMap]
type EvaluationSetup = tuple[RuntimeBindings, Scope]


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


def spawn_workers(runtime: Runtime, worker_args: tuple) -> Generator[Any, Any, None]:
    """Spawn the configured process and thread workers."""

    for _ in range(runtime.n_workers):
        yield ESpawn(fn_name="worker", args=worker_args, mode=SpawnMode.PROCESS)

    for _ in range(runtime.n_thread_workers):
        yield ESpawn(fn_name="worker", args=worker_args, mode=SpawnMode.THREAD)


def _evaluate_runner(
    runtime: Runtime,
    inputs: EvaluationInputs,
    bindings: RuntimeBindings,
) -> Generator[Any, Any, None]:
    """Run the root job and wait for completion."""

    fn_name, args, scope = inputs
    wrappers, overseer_handlers, handlers, providers = bindings

    # Only the overseer holds the storage backend; workers and the root carry
    # user handlers plus transport bindings.
    overseer: Pid = yield ESpawn(fn_name="run_overseer", args=(overseer_handlers,))

    worker_args = (
        bytes(overseer),
        scope,
        wrappers,
        (handlers, providers),
    )

    yield from spawn_workers(runtime, worker_args)

    # Coordination merged last: transported storage tags must beat any storage
    # handlers a user-supplied bag might contain.
    coordination = make_coordination_handlers(overseer, wrappers.handlers)
    root_handlers = merge_handlers(build_handler_map(handlers, wrappers.handlers), coordination)
    require_storage_transported(root_handlers, coordination)

    yield from handle(_kickoff(fn_name, args), root_handlers)


def validate_evaluation(fn_name: str, scope: Scope, handlers: HandlerMap | None) -> None:
    """Reject invalid evaluation inputs before the runtime starts."""

    if fn_name not in scope:
        raise KeyError(f"job {fn_name!r} not found in scope")

    user_storage_tags = [tag for tag in handlers or {} if tag in STORAGE_TAGS]
    if user_storage_tags:
        raise ZahirError(f"user handlers may not bind storage tags: {user_storage_tags}")


def make_evaluation_setup(
    scope: Scope,
    wrappers: InterpreterWrappers,
    handlers: HandlerMap | None,
    providers: BindingMap | None,
) -> EvaluationSetup:
    """Build runtime handlers and add internal jobs to the runtime scope."""

    user_handlers = handlers or {}
    memory_handlers = make_memory_storage_handlers(wrappers.handlers)
    overseer_handlers = merge_handlers(
        memory_handlers, build_handler_map(user_handlers, wrappers.handlers)
    )
    full_scope: Scope = {"run_overseer": run_overseer, "worker": worker, **scope}
    bindings = (
        wrappers,
        overseer_handlers,
        user_handlers,
        providers or {},
    )
    return bindings, full_scope


def evaluate(  # noqa: PLR0913
    runtime: Runtime,
    fn_name: str,
    args: tuple,
    scope: Scope,
    *,
    handler_wrappers: Sequence = (),
    provider_wrappers: Sequence = (),
    handlers: HandlerMap | None = None,
    providers: BindingMap | None = None,
) -> Generator[Any]:
    """Entry point. Run a job and wait for completion."""

    validate_evaluation(fn_name, scope, handlers)
    wrappers = InterpreterWrappers(handler_wrappers, provider_wrappers)
    bindings, full_scope = make_evaluation_setup(
        scope,
        wrappers,
        handlers,
        providers,
    )
    yield from run(
        _evaluate_runner,
        runtime,
        (fn_name, args, scope),
        bindings,
        scope=full_scope,
        transport=runtime.transport,
    )
