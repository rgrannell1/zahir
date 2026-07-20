"""Runtime setup values for evaluate() — local (IPC) and remote-capable (TCP) topologies.

A remote runtime binds the broker over TCP so workers on other hosts can join the
swarm with join_worker(); the overseer is found by its registered broker name, so
no process refs need to be exchanged up front.
"""

from dataclasses import dataclass

from tertius import CurveSecurity, IpcTransport, TcpTransport, Transport


@dataclass(frozen=True)
class Runtime:
    """Topology for one evaluate() run: transport plus worker pool sizes."""

    transport: Transport
    n_workers: int
    n_thread_workers: int


def setup(
    *,
    transport: Transport | None = None,
    n_workers: int = 4,
    n_thread_workers: int = 0,
) -> Runtime:
    """Return a local runtime value for evaluate().

    n_workers spawns OS-process workers; n_thread_workers spawns thread workers
    inside the VM process. Thread workers are cheap to start but share the GIL,
    so they suit I/O-bound jobs.
    """

    selected_transport = transport if transport is not None else IpcTransport()
    return Runtime(selected_transport, n_workers, n_thread_workers)


def setup_remote(  # noqa: PLR0913
    *,
    host: str,
    data_port: int,
    control_port: int,
    security: CurveSecurity | None = None,
    n_workers: int = 0,
    n_thread_workers: int = 0,
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
    return Runtime(transport, n_workers, n_thread_workers)
