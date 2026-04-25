"""UX test: verifies that resource-gated jobs block or fan out based on the threshold."""

import os
import time

import pytest

from tertius import EEmit, ESleep

from zahir.core.dependencies.resources import check_resource_dependency
from zahir.core.effects import EAwait
from zahir.core.evaluate import JobContext, evaluate
from tests.shared import peak_concurrent, user_events

_N_WORKERS = 10
_N_JOBS = 10

# Threshold guaranteed to be impossible regardless of machine state
_ALWAYS_BLOCKED = 0.0

# Threshold guaranteed to be satisfied regardless of machine state
_ALWAYS_SATISFIED = 100.0

# Minimum fraction of workers that must overlap at peak for the fanout assertion
_MIN_FANOUT_FRACTION = 0.8


def ram_limited_job(ctx: JobContext, max_percent: float):
    """A job gated behind a memory resource check. Returns (pid, start, end) on success."""
    result = yield from check_resource_dependency("memory", max_percent=max_percent)
    match result:
        case ("satisfied", _):
            start = time.time()
            yield ESleep(ms=300)
            end = time.time()
            return (os.getpid(), start, end)
        case ("impossible", _):
            return None


def ram_fanout_root(ctx: JobContext, max_percent: float):
    """Root that fans out ten RAM-gated jobs and emits the resulting records."""
    records = yield EAwait([ctx.scope.ram_limited_job(max_percent) for _ in range(_N_JOBS)])
    yield EEmit(records)


_SCOPE = {
    "ram_fanout_root": ram_fanout_root,
    "ram_limited_job": ram_limited_job,
}


@pytest.mark.local_only
def test_resource_dependency_blocks_all_jobs_when_threshold_is_zero():
    """Proves that jobs behind a memory dependency all return None when the threshold is impossible to meet."""

    events = user_events(
        evaluate("ram_fanout_root", (_ALWAYS_BLOCKED,), _SCOPE, n_workers=_N_WORKERS)
    )

    records = events[-1]
    assert all(record is None for record in records), (
        f"expected all jobs to be blocked, got: {records}"
    )


@pytest.mark.local_only
def test_resource_dependency_fans_out_to_multiple_cores_when_threshold_is_always_met():
    """Proves that RAM-gated jobs fan out across the worker pool when the dependency is always satisfied.

    Measures peak concurrent overlap from each job's self-recorded start/end interval —
    unaffected by the event-emission gap during ESleep.
    """

    events = user_events(
        evaluate("ram_fanout_root", (_ALWAYS_SATISFIED,), _SCOPE, n_workers=_N_WORKERS)
    )

    records = events[-1]
    assert all(record is not None for record in records), (
        f"some jobs were blocked unexpectedly: {records}"
    )

    intervals = [(start, end) for _, start, end in records]
    peak = peak_concurrent(intervals)
    min_expected = int(_N_WORKERS * _MIN_FANOUT_FRACTION)
    assert peak >= min_expected, (
        f"peak concurrent jobs {peak} below {min_expected} ({_MIN_FANOUT_FRACTION:.0%} of {_N_WORKERS} workers)"
    )
