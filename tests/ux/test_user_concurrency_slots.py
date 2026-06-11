"""UX test: verifies that concurrency slots are acquired and released correctly."""

from tertius import EEmit, ESleep

from zahir.core.dependencies.concurrency import concurrency_dependency
from zahir.core.effects import await_all
from zahir.core.evaluate import JobContext, evaluate, setup

_SLOT_NAME = "workers"
_LIMIT = 2
_N_JOBS = 6


def slot_job(ctx: JobContext, job_id: int):
    """Acquire a concurrency slot, do work, then finish — slot is released automatically."""
    yield from concurrency_dependency(_SLOT_NAME, limit=_LIMIT)
    yield ESleep(ms=50)
    yield EEmit({"done": job_id})


def fan_out(ctx: JobContext):
    yield await_all([ctx.scope.slot_job(idx) for idx in range(_N_JOBS)])


_SCOPE = {"fan_out": fan_out, "slot_job": slot_job}


def test_all_slots_complete_after_release():
    """Proves all jobs acquire and release their slot so every job eventually runs."""

    events = list(evaluate(setup(n_workers=4), "fan_out", (), _SCOPE))

    done_ids = {event["done"] for event in events if isinstance(event, dict) and "done" in event}
    assert done_ids == set(range(_N_JOBS))
