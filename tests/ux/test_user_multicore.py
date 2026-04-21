"""Integration test: verifies that parallel fan-out distributes work across OS processes."""

import os

from tertius import EEmit, ESleep

from zahir.core.effects import EAwait
from zahir.core.evaluate import JobContext, evaluate

_N_WORKERS = 4
_N_JOBS = 8  # more jobs than workers so at least two workers must be used


def report_pid(ctx: JobContext) -> int:
    # Sleep long enough for other workers to pick up sibling jobs from the queue.
    # Without a pause, one worker drains the queue before others get scheduled.
    yield ESleep(ms=50)
    return os.getpid()


def collect_pids(ctx: JobContext):
    pids = yield EAwait([ctx.scope.report_pid() for _ in range(_N_JOBS)])
    yield EEmit(pids)


_SCOPE = {"collect_pids": collect_pids, "report_pid": report_pid}


def test_parallel_fanout_uses_multiple_os_processes():
    """Proves that fanning out jobs across workers runs on more than one OS process."""

    events = list(evaluate("collect_pids", (), _SCOPE, n_workers=_N_WORKERS))
    pids: list[int] = events[0]

    assert len(pids) == _N_JOBS
    assert len(set(pids)) > 1, f"all jobs ran on the same OS process — got pids: {pids}"
