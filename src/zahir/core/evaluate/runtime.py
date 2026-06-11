"""Runtime setup values for evaluate() — local (IPC) and remote-capable (TCP) topologies.

A remote runtime binds the broker over TCP so workers on other hosts can join the
swarm with join_worker(); the overseer is found by its registered broker name, so
no process refs need to be exchanged up front.
"""

from typing import Any, Literal, TypeGuard

from tertius import CurveSecurity, IpcTransport, TcpTransport, Transport

_RUNTIME_SIZE = 3

type Runtime = tuple[Literal["runtime"], Transport, int]


def setup(
    *,
    transport: Transport | None = None,
    n_workers: int = 4,
) -> Runtime:
    """Return a tagged local runtime value for evaluate()."""

    selected_transport = transport if transport is not None else IpcTransport()
    return ("runtime", selected_transport, n_workers)


def setup_remote(  # noqa: PLR0913
    *,
    host: str,
    data_port: int,
    control_port: int,
    security: CurveSecurity | None = None,
    n_workers: int = 0,
) -> Runtime:
    """Return a tagged runtime value that accepts remote workers over TCP.

    evaluate() binds the broker on host:data_port/control_port; remote hosts then
    call join_worker() against the same address. n_workers local workers are still
    spawned — zero by default, on the assumption the fleet does the work.
    """

    transport = TcpTransport(
        host=host,
        data_port=data_port,
        control_port=control_port,
        security=security,
    )
    return ("runtime", transport, n_workers)


def is_runtime(value: Any) -> TypeGuard[Runtime]:
    """Return True when the value is a tagged runtime tuple."""

    return (
        isinstance(value, tuple)
        and len(value) == _RUNTIME_SIZE
        and value[0] == "runtime"
        and isinstance(value[2], int)
    )
