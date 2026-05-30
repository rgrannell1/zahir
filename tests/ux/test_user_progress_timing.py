"""UX tests proving the progress bar measures job sleep duration correctly via telemetry."""

import re

from bookman.events import Event
from tertius import ESleep

from zahir.core.effects import await_all
from zahir.core.evaluate import JobContext, evaluate
from zahir.core.telemetry import make_telemetry
from zahir.progress_bar.progress_bar_service import ProgressBarService

_HH_MM_SS = re.compile(r"\d{2}:\d{2}:\d{2}")


def sleeping_job(ctx: JobContext):
    """A job that sleeps for 1 second."""
    yield ESleep(ms=1_000)
    return "done"


def sleep_workflow(ctx: JobContext):
    """Root workflow that dispatches a single sleeping job."""
    result = yield ctx.scope.sleeping_job()
    return result


SLEEP_SCOPE = {
    "sleep_workflow": sleep_workflow,
    "sleeping_job": sleeping_job,
}

# Job sleeps 1 second; allow up to 1.5s for scheduling overhead
_SLEEP_LOWER_BOUND_MS = 500.0
_SLEEP_UPPER_BOUND_MS = 1_500.0


def _collect_service(fn_name: str, args: tuple, scope: dict) -> ProgressBarService:
    """Run a workflow and return the accumulated progress bar service state."""
    service = ProgressBarService()
    for event in evaluate(fn_name, args, scope, n_workers=4, handler_wrappers=[make_telemetry()]):
        if isinstance(event, Event):
            service.update(event)
    return service


def test_sleeping_job_duration_measured_by_progress_bar():
    """Proves a job sleeping 1 second is measured as approximately 1 second by the progress bar."""

    service = _collect_service("sleep_workflow", (), SLEEP_SCOPE)

    duration = service.mean_duration_ms("sleeping_job")
    assert duration is not None, "no duration recorded for sleeping_job"
    lower_fmt = f"{_SLEEP_LOWER_BOUND_MS:.0f}"
    upper_fmt = f"{_SLEEP_UPPER_BOUND_MS:.0f}"
    duration_fmt = f"{duration:.1f}"
    assert (
        _SLEEP_LOWER_BOUND_MS <= duration <= _SLEEP_UPPER_BOUND_MS
    ), f"expected duration in [{lower_fmt}, {upper_fmt}] ms, got {duration_fmt} ms"


def short_job(ctx: JobContext):
    """A job that sleeps briefly so completed durations are observable before all jobs finish."""
    yield ESleep(ms=150)


def ten_short_jobs(ctx: JobContext):
    """Root workflow that fans out ten short jobs concurrently."""
    yield await_all([ctx.scope.short_job() for _ in range(10)])


_TEN_SCOPE = {
    "ten_short_jobs": ten_short_jobs,
    "short_job": short_job,
}


def test_ten_jobs_eta_renders_hh_mm_ss_during_execution():
    """Proves that running ten jobs causes the ETA to render as HH:MM:SS at least once."""

    service = ProgressBarService()
    eta_snapshots = set()

    telemetry = make_telemetry()
    result = evaluate("ten_short_jobs", (), _TEN_SCOPE, n_workers=4, handler_wrappers=[telemetry])
    for event in result:
        if isinstance(event, Event):
            service.update(event)
            enqueued = [stat for stat in service.jobs.values() if stat.total > 0]
            completed = sum(stat.processed for stat in enqueued)
            remaining = sum(stat.total for stat in enqueued) - completed
            eta_snapshots.add(service.format_eta(completed, remaining))

    rendered = {eta for eta in eta_snapshots if _HH_MM_SS.fullmatch(eta)}
    assert rendered, f"ETA never rendered a time estimate; observed: {eta_snapshots}"
