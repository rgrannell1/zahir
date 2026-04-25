# Overseer gen_server — serialises all coordination state access across workers.
from collections.abc import Generator
from typing import Any

from orbis import handle
from tertius import gen_server

from zahir.core.effects import EStorageInitialize


def _init(initial_fn: str, initial_args: tuple) -> Generator[Any, Any, None]:
    """Seed the backend with the root job via a storage effect."""
    yield EStorageInitialize(initial_fn, initial_args)
    return None


def _handle_call(state: Any, body: Any) -> Generator[Any, Any, tuple[Any, Any]]:
    """Pass the storage effect through to the handle() layer and return the result."""
    result = yield body
    return state, result


def _handle_cast(state: Any, body: Any) -> Generator[Any, Any, Any]:
    """Pass the storage effect through to the handle() layer."""
    yield body
    return state


def run_overseer(
    initial_fn: str,
    initial_args: tuple,
    storage_handlers: dict,
) -> Generator[Any, Any, None]:
    overseer = gen_server(
        init=_init,
        handle_call=_handle_call,
        handle_cast=_handle_cast,
    )
    yield from handle(overseer(initial_fn, initial_args), **storage_handlers)
