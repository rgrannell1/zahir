"""Our overseer genserver delegates all work to storage handlers & further downstreams."""

from collections.abc import Generator
from typing import Any

from orbis import handle
from tertius import ERegister, gen_server

from zahir.core.constants import OVERSEER_NAME


def _init() -> Generator[Any, Any, None]:
    """Register the overseer's broker name so remote workers can find it.

    The root job is enqueued by _root after all workers are spawned.
    """

    yield ERegister(name=OVERSEER_NAME)
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
    handlers: dict,
) -> Generator[Any, Any, None]:
    """Run the overseer genserver. Receives the full handler bag; dispatches its storage slice."""

    overseer = gen_server(
        init=_init,
        handle_call=_handle_call,
        handle_cast=_handle_cast,
    )

    yield from handle(overseer(), **handlers)
