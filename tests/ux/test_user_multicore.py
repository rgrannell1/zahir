"""Integration test: verifies that parallel fan-out distributes work across OS processes."""

import os
from collections.abc import Generator
from typing import Any

from bookman.events import Event
from tertius import EEmit, ESleep

from tests.shared import user_events
from zahir.core.constants import JobTag, Phase
from zahir.core.effects import await_all
from zahir.core.evaluate import JobContext, evaluate, setup
from zahir.core.telemetry import make_telemetry

_N_WORKERS = 4
_N_JOBS = 8  # more jobs than workers so at least two workers must be used


def report_pid(ctx: JobContext) -> Generator[Any, Any, int]:
    # Sleep long enough for other workers to pick up sibling jobs from the queue.
    # Without a pause, one worker drains the queue before others get scheduled.
    yield ESleep(ms=50)
    return os.getpid()


def collect_pids(ctx: JobContext):
    pids = yield await_all([ctx.scope.report_pid() for _ in range(_N_JOBS)])
    yield EEmit(pids)


_SCOPE = {"collect_pids": collect_pids, "report_pid": report_pid}


def test_parallel_fanout_uses_multiple_os_processes():
    """Proves that fanning out jobs across workers runs on more than one OS process."""

    events = user_events(evaluate(setup(n_workers=_N_WORKERS), "collect_pids", (), _SCOPE))
    pids: list[int] = events[0]

    assert len(pids) == _N_JOBS
    assert len(set(pids)) > 1, f"all jobs ran on the same OS process — got pids: {pids}"


def test_job_ids_are_unique_across_workers():
    """Proves job_id dimensions on enqueue events are globally unique across workers."""

    telemetry = make_telemetry()
    wrappers = [telemetry]
    result = evaluate(
        setup(n_workers=_N_WORKERS),
        "collect_pids",
        (),
        _SCOPE,
        handler_wrappers=wrappers,
    )
    raw_events = list(result)

    enqueue_starts = [
        event
        for event in raw_events
        if isinstance(event, Event)
        and event.dim("tag") == JobTag.ENQUEUE
        and event.dim("phase") == Phase.START
        and event.dim("job_id") is not None
    ]

    job_ids = [event.dim("job_id") for event in enqueue_starts]
    assert len(job_ids) == len(set(job_ids)), f"duplicate job_ids found: {job_ids}"
