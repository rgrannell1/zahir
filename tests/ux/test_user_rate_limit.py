"""UX test: proves n jobs gated by rate_limit_dependency complete in at least (n-1) * min_seconds."""

import time

from tertius import EEmit

from zahir.core.dependencies.rate_limit import rate_limit_dependency
from zahir.core.effects import await_all
from zahir.core.evaluate import JobContext, evaluate

_RATE_NAME = "ux_test_rate_limit"
_MIN_SECONDS = 0.3
_N_JOBS = 20

# Each extra job must wait at least one min_seconds gap before satisfying.
_EXPECTED_MIN_SPAN_S = (_N_JOBS - 1) * _MIN_SECONDS
# Generous upper bound to absorb scheduling overhead.
_EXPECTED_MAX_SPAN_S = _EXPECTED_MIN_SPAN_S * 6.0


def rate_limited_job(ctx: JobContext, job_id: int):
    """Acquire the rate limit then record when it was satisfied."""
    yield from rate_limit_dependency(_RATE_NAME, min_seconds=_MIN_SECONDS)
    yield EEmit({"job_id": job_id, "satisfied_at": time.time()})


def fan_out(ctx: JobContext):
    yield await_all([ctx.scope.rate_limited_job(idx) for idx in range(_N_JOBS)])


_SCOPE = {"fan_out": fan_out, "rate_limited_job": rate_limited_job}


def _satisfaction_times(events: list) -> list[float]:
    """Extract sorted satisfaction timestamps from emitted events."""
    times = [e["satisfied_at"] for e in events if isinstance(e, dict) and "satisfied_at" in e]
    times.sort()
    return times


def test_rate_limited_jobs_are_spaced_by_at_least_min_seconds():
    """Proves n jobs gated by rate_limit_dependency take at least (n-1) * min_seconds in total.

    Uses n_workers=1 so jobs execute sequentially within the single worker — each job
    acquires the mutex and then sleeps via ESleep(remaining) rather than competing on
    'slot busy' retries, giving clean and deterministic timing.
    """
    events = list(evaluate("fan_out", (), _SCOPE, n_workers=1))

    times = _satisfaction_times(events)
    assert len(times) == _N_JOBS, f"expected {_N_JOBS} satisfaction events, got {len(times)}"

    total_span = times[-1] - times[0]
    assert total_span >= _EXPECTED_MIN_SPAN_S, (
        f"total span {total_span:.3f}s < expected minimum {_EXPECTED_MIN_SPAN_S:.2f}s — "
        f"rate limit is not enforcing the gap"
    )
    assert total_span <= _EXPECTED_MAX_SPAN_S, (
        f"total span {total_span:.3f}s > expected maximum {_EXPECTED_MAX_SPAN_S:.2f}s — "
        f"rate limit is taking unexpectedly long"
    )

    gaps = [times[idx + 1] - times[idx] for idx in range(len(times) - 1)]
    for idx, gap in enumerate(gaps):
        assert gap >= _MIN_SECONDS * 0.8, (
            f"gap between jobs {idx} and {idx + 1} was {gap:.3f}s, "
            f"expected >= {_MIN_SECONDS * 0.8:.3f}s"
        )
