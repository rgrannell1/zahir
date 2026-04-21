"""UX test: verifies that concurrency slots are acquired and released correctly."""

from tertius import EEmit, ESleep

from zahir.core.dependencies.concurrency import concurrency_dependency
from zahir.core.effects import EAwait
from zahir.core.evaluate import JobContext, evaluate

_SLOT_NAME = "workers"
_LIMIT = 2
_N_JOBS = 6


def slot_job(ctx: JobContext, job_id: int):
    """Acquire a concurrency slot, do work, then finish — slot is released automatically."""
    yield from concurrency_dependency(_SLOT_NAME, limit=_LIMIT)
    yield ESleep(ms=50)
    yield EEmit({"done": job_id})


def fan_out(ctx: JobContext):
    yield EAwait([ctx.scope.slot_job(idx) for idx in range(_N_JOBS)])


_SCOPE = {"fan_out": fan_out, "slot_job": slot_job}


def test_all_slots_complete_after_release():
    """Proves all jobs acquire and release their slot so every job eventually runs."""

    events = list(evaluate("fan_out", (), _SCOPE, n_workers=4))

    done_ids = {e["done"] for e in events if isinstance(e, dict) and "done" in e}
    assert done_ids == set(range(_N_JOBS))
