# Overseer gen_server — serialises all coordination state access across workers.
from collections.abc import Generator
from typing import Any

from orbis import handle
from tertius import gen_server


def _init() -> Generator[Any, Any, None]:
    """No-op init — the root job is enqueued by _root after all workers are spawned."""
    return None
    yield


def _handle_call(state: Any, body: Any) -> Generator[Any, Any, tuple[Any, Any]]:
    """Pass the storage effect through to the handle() layer and return the result."""
    result = yield body
    return state, result


def _handle_cast(state: Any, body: Any) -> Generator[Any, Any, Any]:
    """Pass the storage effect through to the handle() layer."""
    yield body
    return state


def run_overseer(
    storage_handlers: dict,
) -> Generator[Any, Any, None]:
    overseer = gen_server(
        init=_init,
        handle_call=_handle_call,
        handle_cast=_handle_cast,
    )
    yield from handle(overseer(), **storage_handlers)
