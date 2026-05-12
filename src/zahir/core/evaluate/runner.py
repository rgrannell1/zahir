"""Entry point for running a zahir workflow: spawns the overseer and workers, seeds the root job, and polls for completion."""

from collections.abc import Generator, Sequence
from dataclasses import dataclass, field
from typing import Any, cast

from orbis import handle
from tertius import EEmit, ESleep, ESpawn, Pid, Scope, run

from zahir.core.backends.memory import make_memory_storage_handlers
from zahir.core.constants import COMPLETION_POLL_MS
from zahir.core.effects import EEnqueue, EGetError, EGetResult, EIsDone
from zahir.core.evaluate.coordination_handlers import make_merged_coordination_handlers
from zahir.core.evaluate.overseer import run_overseer
from zahir.core.evaluate.worker import worker
from zahir.core.zahir_types import HandlerMap


@dataclass
class EvaluateConfig:
    """Runtime configuration for a zahir evaluation."""

    n_workers: int = 4
    handler_wrappers: Sequence = field(default_factory=tuple)
    handlers: HandlerMap | None = None


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


def _evaluate_runner(fn_name: str, args: tuple, scope: Scope, config: EvaluateConfig) -> Generator[Any, Any, None]:
    """Run the root job and wait for completion."""

    overseer: Pid = yield ESpawn(fn_name="run_overseer", args=(config.handlers,))

    for _ in range(config.n_workers):
        yield ESpawn(fn_name="worker", args=(bytes(overseer), scope, config.handler_wrappers, config.handlers))

    root_handlers = make_merged_coordination_handlers(overseer, config.handler_wrappers, config.handlers)

    yield from handle(_kickoff(fn_name, args), **root_handlers)


def evaluate(  # noqa: PLR0913
    fn_name: str,
    args: tuple,
    scope: Scope,
    *,
    n_workers: int = 4,
    handler_wrappers: Sequence = (),
    handlers: HandlerMap | None = None,
) -> Generator[Any, None, None]:
    """Entry point. Run a job and wait for completion."""

    if fn_name not in scope:
        raise KeyError(f"job {fn_name!r} not found in scope")

    merged_handlers = cast(HandlerMap, {**make_memory_storage_handlers(), **(handlers or {})})
    full_scope: Scope = {"run_overseer": run_overseer, "worker": worker, **scope}
    config = EvaluateConfig(n_workers=n_workers, handler_wrappers=handler_wrappers, handlers=merged_handlers)

    yield from run(_evaluate_runner, fn_name, args, scope, config, scope=full_scope)
