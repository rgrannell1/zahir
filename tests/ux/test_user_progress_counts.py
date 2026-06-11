"""UX tests proving the progress bar counts jobs correctly via telemetry."""

import contextlib

from bookman.events import Event

from tests.ux.test_user_mirror_workflow import BASE_SCOPE
from zahir.core.constants import JobTag, Phase
from zahir.core.effects import await_all
from zahir.core.evaluate import JobContext, evaluate, setup
from zahir.core.exceptions import JobError
from zahir.core.telemetry import make_telemetry
from zahir.progress_bar.progress_bar_state_model import ProgressBarState

_EXPECTED_OK_JOB_COUNT = 2


def _collect_state(fn_name, args, scope) -> ProgressBarState:
    """Run a workflow and return the accumulated progress bar state."""
    state = ProgressBarState()
    runtime = setup(n_workers=4)
    for event in evaluate(runtime, fn_name, args, scope, handler_wrappers=[make_telemetry()]):
        if isinstance(event, Event):
            state.update(event)
    return state


# --- mirror-like workflow counts ---


def test_mirror_workflow_counts_each_job_once():
    """Proves each job in the mirror-shaped workflow is counted exactly once."""

    state = _collect_state("mirror_workflow", ({"publish_d1": False},), BASE_SCOPE)

    for fn_name in (
        "scan_media",
        "media_scan",
        "read_albums",
        "read_photos",
        "read_videos",
        "wikidata_scan",
        "upload_media",
        "publish_artifacts",
    ):
        stats = state.jobs.get(fn_name)
        assert stats is not None, f"{fn_name!r} not found in progress bar state"
        assert stats.total == 1, f"{fn_name!r}: expected total=1, got {stats.total}"
        assert stats.completed == 1, f"{fn_name!r}: expected completed=1, got {stats.completed}"
        assert stats.failed == 0, f"{fn_name!r}: expected failed=0, got {stats.failed}"


def test_mirror_workflow_root_job_is_counted():
    """Proves the root job appears in stats like any other enqueued job."""

    state = _collect_state("mirror_workflow", ({"publish_d1": False},), BASE_SCOPE)

    root_stats = state.jobs.get("mirror_workflow")
    assert root_stats is not None
    assert root_stats.total == 1
    assert root_stats.started == 1
    assert root_stats.completed == 1


# --- mixed ok and failing jobs ---


def ok_job(ctx: JobContext, value: int):
    yield from ()
    return value * 2  # noqa: B901


def bomb_job(ctx: JobContext):
    yield from ()
    raise ValueError("intentional failure")


def mixed_root(ctx: JobContext):
    """Fan out to 2 ok jobs and 1 failing job, swallow the error."""
    with contextlib.suppress(JobError):
        yield await_all(
            [
                ctx.scope.ok_job(1),
                ctx.scope.bomb_job(),
                ctx.scope.ok_job(2),
            ]
        )


MIXED_SCOPE = {
    "mixed_root": mixed_root,
    "ok_job": ok_job,
    "bomb_job": bomb_job,
}


def test_mixed_workflow_counts_successes_and_failures_separately():
    """Proves completed and failed counts are tracked independently in a mixed fan-out."""

    state = _collect_state("mixed_root", (), MIXED_SCOPE)

    ok_stats = state.jobs.get("ok_job")
    assert ok_stats is not None
    assert ok_stats.total == _EXPECTED_OK_JOB_COUNT
    assert ok_stats.completed == _EXPECTED_OK_JOB_COUNT
    assert ok_stats.failed == 0

    bomb_stats = state.jobs.get("bomb_job")
    assert bomb_stats is not None
    assert bomb_stats.total == 1
    assert bomb_stats.completed == 0
    assert bomb_stats.failed == 1


def test_mixed_workflow_enqueued_jobs_all_processed():
    """Proves every enqueued job (total > 0) is fully processed when the workflow ends."""

    state = _collect_state("mixed_root", (), MIXED_SCOPE)

    # exclude the root job itself, which is never enqueued
    enqueued = {fn: stats for fn, stats in state.jobs.items() if stats.total > 0}
    for fn_name, stats in enqueued.items():
        processed = stats.processed
        total = stats.total
        assert processed == total, f"{fn_name!r}: processed={processed} != total={total}"


def test_root_fanout_child_enqueue_telemetry_arrives_before_completion():
    """Proves live progress can see child jobs as soon as a root job fans out."""

    runtime = setup(n_workers=4)
    events = evaluate(runtime, "mixed_root", (), MIXED_SCOPE, handler_wrappers=[make_telemetry()])

    for event in events:
        if (
            isinstance(event, Event)
            and event.dim("tag") == JobTag.ENQUEUE
            and event.dim("phase") == Phase.START
            and event.dim("fn") == "ok_job"
        ):
            return

    raise AssertionError("child enqueue telemetry for ok_job was not emitted")
