from collections import deque
from collections.abc import Generator
from typing import Any

from tertius import gen_server

from zahir.core.evaluate.overseer_handlers import (
    CALL_HANDLERS,
    CAST_HANDLERS,
    _dispatch,
)
from zahir.core.zahir_types import JobSpec, OverseerState


def _init(initial_fn: str, initial_args: tuple) -> OverseerState:
    job = JobSpec(fn_name=initial_fn, args=initial_args, reply_to=None)
    return OverseerState(
        queue=deque([job]),
        concurrency={},
        semaphores={},
        pending=1,
    )


def _handle_call(state: OverseerState, body: Any) -> tuple[OverseerState, Any]:
    return _dispatch(CALL_HANDLERS, state, body)


def _handle_cast(state: OverseerState, body: Any) -> OverseerState:
    return _dispatch(CAST_HANDLERS, state, body)


_overseer = gen_server(init=_init, handle_call=_handle_call, handle_cast=_handle_cast)


def run_overseer(initial_fn: str, initial_args: tuple) -> Generator[Any, Any, None]:
    yield from _overseer(initial_fn, initial_args)
