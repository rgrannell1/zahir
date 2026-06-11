"""UX test: verifies that resource-gated jobs block or fan out based on the threshold."""

import os
import time

import pytest
from tertius import EEmit, ESleep

from tests.shared import peak_concurrent, user_events
from zahir.core.dependencies.resources import check_resource_dependency
from zahir.core.effects import await_all
from zahir.core.evaluate import JobContext, evaluate, setup

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
            return None  # noqa: B901


def ram_fanout_root(ctx: JobContext, max_percent: float):
    """Root that fans out ten RAM-gated jobs and emits the resulting records."""
    records = yield await_all([ctx.scope.ram_limited_job(max_percent) for _ in range(_N_JOBS)])
    yield EEmit(records)


_SCOPE = {
    "ram_fanout_root": ram_fanout_root,
    "ram_limited_job": ram_limited_job,
}


@pytest.mark.local_only
def test_resource_dependency_blocks_all_jobs_when_threshold_is_zero():
    """Proves memory-gated jobs return None when the threshold is impossible to meet."""

    result = evaluate(setup(n_workers=_N_WORKERS), "ram_fanout_root", (_ALWAYS_BLOCKED,), _SCOPE)
    events = user_events(result)

    records = events[-1]
    all_none = all(record is None for record in records)
    assert all_none, f"expected all jobs to be blocked, got: {records}"


@pytest.mark.local_only
def test_resource_dependency_fans_out_to_multiple_cores_when_threshold_is_always_met():
    """Proves RAM-gated jobs fan out across workers when the dependency is satisfied.

    Measures peak concurrent overlap from job's self-recorded start/end intervals.
    """

    result = evaluate(
        setup(n_workers=_N_WORKERS),
        "ram_fanout_root",
        (_ALWAYS_SATISFIED,),
        _SCOPE,
    )
    events = user_events(result)

    records = events[-1]
    none_blocked = all(record is not None for record in records)
    assert none_blocked, f"some jobs were blocked unexpectedly: {records}"

    intervals = [(start, end) for _, start, end in records]
    peak = peak_concurrent(intervals)
    min_expected = int(_N_WORKERS * _MIN_FANOUT_FRACTION)
    frac_pct = f"{_MIN_FANOUT_FRACTION:.0%}"
    assert (
        peak >= min_expected
    ), f"peak concurrent jobs {peak} below {min_expected} ({frac_pct} of {_N_WORKERS} workers)"
