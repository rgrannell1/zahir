"""Remote worker entrypoint — join a running zahir swarm from another host.

The orchestrating host runs evaluate() with a setup_remote() runtime, which binds
the broker over TCP. Each remote host then calls join_worker() against that
address: it joins the broker, resolves the overseer by its registered name, and
runs the standard worker loop, pulling jobs until the swarm shuts down.
"""

from collections.abc import Generator, Sequence
from typing import Any, cast

from tertius import CurveSecurity, ESleep, EWhereis, Pid, Scope, TcpTransport, join

from zahir.core.backends.memory import make_memory_storage_handlers
from zahir.core.combinators import merge_handlers
from zahir.core.constants import (
    OVERSEER_NAME,
    OVERSEER_WHEREIS_POLL_MS,
    OVERSEER_WHEREIS_TIMEOUT_MS,
    REMOTE_RECV_TIMEOUT_MS,
)
from zahir.core.evaluate.worker import worker
from zahir.core.exceptions import OverseerNotFoundError
from zahir.core.zahir_types import HandlerMap


def resolve_overseer(timeout_ms: int) -> Generator[Any, Any, Pid]:
    """Poll the broker for the overseer's registered name until found or timed out."""

    waited_ms = 0
    while waited_ms < timeout_ms:
        pid = yield EWhereis(name=OVERSEER_NAME)
        if pid is not None:
            return pid

        yield ESleep(ms=OVERSEER_WHEREIS_POLL_MS)
        waited_ms += OVERSEER_WHEREIS_POLL_MS

    raise OverseerNotFoundError(
        f"overseer {OVERSEER_NAME!r} not registered within {timeout_ms}ms"
    )


def _joined_worker(
    scope: Scope,
    handler_wrappers: Sequence,
    handlers: HandlerMap,
    overseer_timeout_ms: int,
) -> Generator[Any, Any, None]:
    """Resolve the overseer by name, then run the standard worker loop against it."""

    overseer = yield from resolve_overseer(overseer_timeout_ms)
    yield from worker(bytes(overseer), scope, handler_wrappers, handlers)


def join_worker(  # noqa: PLR0913
    *,
    host: str,
    data_port: int,
    control_port: int,
    scope: Scope,
    security: CurveSecurity | None = None,
    handler_wrappers: Sequence = (),
    handlers: HandlerMap | None = None,
    overseer_timeout_ms: int = OVERSEER_WHEREIS_TIMEOUT_MS,
    recv_timeout_ms: int = REMOTE_RECV_TIMEOUT_MS,
) -> None:
    """Run a zahir worker that joins a remote swarm over TCP. Blocks until shutdown.

    The scope must contain the same job functions as the orchestrator's — jobs are
    dispatched by name, so the code has to be installed on this host. The call only
    returns by raising: OverseerNotFoundError when no overseer registers in time,
    or a receive timeout error once the orchestrator shuts down and stops replying
    for recv_timeout_ms.
    """

    transport = TcpTransport(
        host=host,
        data_port=data_port,
        control_port=control_port,
        security=security,
    )
    memory_handlers = make_memory_storage_handlers(handler_wrappers)
    merged_handlers = cast(HandlerMap, merge_handlers(memory_handlers, handlers or {}))

    join(
        _joined_worker,
        scope,
        handler_wrappers,
        merged_handlers,
        overseer_timeout_ms,
        transport=transport,
        recv_timeout_ms=recv_timeout_ms,
    )
