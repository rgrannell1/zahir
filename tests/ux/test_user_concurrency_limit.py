"""UX test: verifies that the concurrency slot limit is never exceeded across many processes."""

import time

from tertius import EEmit, ESleep

from zahir.core.dependencies.concurrency import concurrency_dependency
from zahir.core.effects import EAwait
from zahir.core.evaluate import JobContext, evaluate

_SLOT_NAME = "bounded"
_LIMIT = 3
_N_JOBS = 12
_HOLD_MS = 60  # long enough that multiple jobs are in-flight at once


def bounded_job(ctx: JobContext, job_id: int):
    """Acquire slot, emit bracketing events while holding it, then finish."""
    yield from concurrency_dependency(_SLOT_NAME, limit=_LIMIT)
    yield EEmit({"event": "acquired", "id": job_id, "ts": time.time()})
    yield ESleep(ms=_HOLD_MS)
    yield EEmit({"event": "released", "id": job_id, "ts": time.time()})


def fan_out(ctx: JobContext):
    yield EAwait([ctx.scope.bounded_job(idx) for idx in range(_N_JOBS)])


_SCOPE = {"fan_out": fan_out, "bounded_job": bounded_job}


def _span_events(events: list) -> list[tuple[float, str]]:
    """Extract (timestamp, event) pairs from acquired/released emit events, sorted by time."""
    spans = [
        (e["ts"], e["event"])
        for e in events
        if isinstance(e, dict) and "event" in e and "ts" in e
    ]
    spans.sort()
    return spans


def _peak_concurrent(spans: list[tuple[float, str]]) -> int:
    """Walk the sorted timeline and return the highest simultaneous holder count."""
    count = 0
    peak = 0
    for _, event in spans:
        if event == "acquired":
            count += 1
            peak = max(peak, count)
        else:
            count -= 1
    return peak


def test_concurrency_limit_is_never_exceeded():
    """Proves the concurrency slot limit is never exceeded across many parallel processes."""

    # n_workers > _LIMIT so without the semaphore, more than _LIMIT would overlap
    events = list(evaluate("fan_out", (), _SCOPE, n_workers=8))

    spans = _span_events(events)
    assert len(spans) == _N_JOBS * 2, f"expected {_N_JOBS * 2} bracket events, got {len(spans)}"

    peak = _peak_concurrent(spans)
    assert peak > 0, "no jobs ever acquired the slot"
    assert peak <= _LIMIT, f"concurrency limit {_LIMIT} exceeded — peak was {peak}"
