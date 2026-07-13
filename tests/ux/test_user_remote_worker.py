"""UX test: a worker in a separate OS process joins a remote swarm and executes its jobs."""

import multiprocessing
import os

import pytest
from tertius import JoinTimeoutError

from tests.shared import free_ports, user_events
from zahir.core.effects import await_all
from zahir.core.evaluate import JobContext, evaluate, join_worker, setup_remote

_SPAWN_CTX = multiprocessing.get_context("spawn")


def remote_leaf(ctx: JobContext, value: int):
    """Double the value and report which OS process executed it."""

    yield from ()
    return {"value": value * 2, "worker_os_pid": os.getpid()}


def remote_root(ctx: JobContext, values: tuple):
    """Fan the values out to leaf jobs and collect their results."""

    results = yield await_all([ctx.scope.remote_leaf(value) for value in values])
    return results
    yield


_SCOPE = {"remote_root": remote_root, "remote_leaf": remote_leaf}


def _run_join_worker(data_port: int, control_port: int) -> None:
    """Joined-worker OS-process entry. Exits once the orchestrator shuts down."""

    try:
        join_worker(
            host="127.0.0.1",
            data_port=data_port,
            control_port=control_port,
            scope=_SCOPE,
            recv_timeout_ms=5_000,
        )
    except Exception:  # noqa: BLE001
        # Swarm shutdown surfaces as a receive timeout — expected, exit cleanly.
        return


def _evaluate_with_joined_worker(values: tuple) -> tuple[list, int]:
    """Run a remote swarm with one joined worker; return (user events, joiner OS pid)."""

    data_port, control_port = free_ports(2)
    joiner = _SPAWN_CTX.Process(target=_run_join_worker, args=(data_port, control_port))
    joiner.start()
    assert joiner.pid is not None

    runtime = setup_remote(host="127.0.0.1", data_port=data_port, control_port=control_port)
    try:
        events = user_events(evaluate(runtime, "remote_root", (values,), _SCOPE))
    finally:
        joiner.join(timeout=15)
        if joiner.is_alive():
            joiner.terminate()

    return events, joiner.pid


def test_remote_worker_executes_jobs_from_another_process():
    """Proves a join_worker() process executes a remote swarm's jobs with n_workers=0."""

    events, joiner_pid = _evaluate_with_joined_worker((1, 2, 3))

    assert len(events) == 1
    results = events[0]
    assert [result["value"] for result in results] == [2, 4, 6]

    worker_os_pids = {result["worker_os_pid"] for result in results}
    assert worker_os_pids == {joiner_pid}, "all jobs should run in the joined worker process"
    assert os.getpid() not in worker_os_pids


def test_join_worker_fails_fast_without_a_swarm():
    """Proves join_worker() raises JoinTimeoutError when no orchestrator is listening."""

    data_port, control_port = free_ports(2)

    with pytest.raises(JoinTimeoutError):
        join_worker(
            host="127.0.0.1",
            data_port=data_port,
            control_port=control_port,
            scope=_SCOPE,
            overseer_timeout_ms=1_000,
            recv_timeout_ms=1_000,
        )
