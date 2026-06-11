"""UX test: verifies local and remote setup helpers produce working runtime handles."""

from tertius import TcpTransport

from tests.shared import free_ports, user_events
from zahir.core.evaluate import JobContext, evaluate, setup, setup_remote


def setup_returning_root(ctx: JobContext):
    assert ctx is not None
    yield from ()
    return {"result": 42}  # noqa: B901


_SCOPE = {"setup_returning_root": setup_returning_root}


def test_setup_runtime_runs_the_existing_local_path():
    """Proves evaluate(setup(...), ...) preserves the current local execution path."""

    runtime = setup(n_workers=1)

    events = user_events(evaluate(runtime, "setup_returning_root", (), _SCOPE))

    assert events == [{"result": 42}]


def test_setup_remote_evaluates_over_tcp_transport():
    """Proves a setup_remote runtime binds the broker over TCP and evaluates jobs through it."""

    data_port, control_port = free_ports(2)
    runtime = setup_remote(
        host="127.0.0.1",
        data_port=data_port,
        control_port=control_port,
        n_workers=1,
    )

    assert isinstance(runtime[1], TcpTransport)

    events = user_events(evaluate(runtime, "setup_returning_root", (), _SCOPE))

    assert events == [{"result": 42}]
