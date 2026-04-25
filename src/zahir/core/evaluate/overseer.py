from collections.abc import Generator, Sequence
from functools import partial, reduce
from typing import Any

from tertius import gen_server

from zahir.core.combinators import wrap_overseer
from zahir.core.evaluate.overseer_handlers import (
    CALL_HANDLERS,
    CAST_HANDLERS,
    _dispatch,
)


def _init(backend, initial_fn: str, initial_args: tuple) -> Generator[Any, Any, Any]:
    """Seed the backend with the root job and return it as the initial gen_server state."""
    backend.initialize(initial_fn, initial_args)
    return backend
    yield


def _handle_call(call_handlers: dict, state: OverseerState, body: Any) -> Generator[Any, Any, tuple[OverseerState, Any]]:
    return (yield from _dispatch(call_handlers, state, body))


def _handle_cast(cast_handlers: dict, state: OverseerState, body: Any) -> Generator[Any, Any, OverseerState]:
    return (yield from _dispatch(cast_handlers, state, body))


def _apply_one_overseer_wrapper(key: str, handler, wrapper):
    """Apply a single handler_wrapper to one overseer gen_server handler."""

    return wrap_overseer(wrapper.args[0])(key, handler)


def _apply_overseer_wrappers(key: str, handler, wrappers: Sequence):
    """Fold all handler_wrappers over a single overseer gen_server handler."""

    return reduce(partial(_apply_one_overseer_wrapper, key), wrappers, handler)


def _wrap_overseer_handlers(handlers: dict, wrappers: Sequence) -> dict:
    """Apply handler_wrappers to each overseer gen_server handler."""

    return {key: _apply_overseer_wrappers(key, handler, wrappers) for key, handler in handlers.items()}


def run_overseer(initial_fn: str, initial_args: tuple, handler_wrappers, backend) -> Generator[Any, Any, None]:
    call_handlers = _wrap_overseer_handlers(CALL_HANDLERS, handler_wrappers)
    cast_handlers = _wrap_overseer_handlers(CAST_HANDLERS, handler_wrappers)

    overseer = gen_server(
        init=partial(_init, backend),
        handle_call=partial(_handle_call, call_handlers),
        handle_cast=partial(_handle_cast, cast_handlers),
    )
    yield from overseer(initial_fn, initial_args)
