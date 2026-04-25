import time

from bookman.events import point, span
from zahir.core.constants import JobTag, Phase
from zahir.progress_bar.system_stats_service import SystemStats


def _job_start(span_id: str, pid: int = 1234, fn: str = "some_job"):
    """ENQUEUE+START event — worker has taken a job."""
    return point(
        {"tag": [JobTag.ENQUEUE], "phase": [Phase.START], "id": [span_id], "pid": [str(pid)], "fn": [fn]},
        at=time.time(),
    )


def _job_end(span_id: str, pid: int = 1234, fn: str = "some_job"):
    """JOB_COMPLETE+END event — worker has finished a job."""
    return point(
        {"tag": [JobTag.JOB_COMPLETE], "phase": [Phase.END], "id": [span_id], "pid": [str(pid)], "fn": [fn]},
        at=time.time(),
    )


def _job_fail(span_id: str, pid: int = 1234, fn: str = "some_job"):
    """JOB_FAIL+END event — worker's job errored."""
    return point(
        {"tag": [JobTag.JOB_FAIL], "phase": [Phase.END], "id": [span_id], "pid": [str(pid)], "fn": [fn]},
        at=time.time(),
    )


def _idle_event(span_id: str, pid: int = 1234):
    """Event without fn set — represents an idle worker polling for jobs."""
    return point({"tag": ["t"], "id": [span_id], "pid": [str(pid)]}, at=time.time())


def test_active_cores_zero_with_no_events():
    "Proves active_cores is zero before any events arrive"
    stats = SystemStats()
    assert stats.active_cores == 0


def test_active_cores_increments_on_job_start():
    "Proves a job start event registers a core as active"
    stats = SystemStats()
    stats.update(_job_start("span-1", pid=100))
    assert stats.active_cores == 1


def test_idle_event_does_not_register_core():
    "Proves an event without fn does not count as an active core"
    stats = SystemStats()
    stats.update(_idle_event("span-1", pid=100))
    assert stats.active_cores == 0


def test_active_cores_stays_active_during_long_job():
    "Proves a pid remains active between its start and end events"
    stats = SystemStats()
    stats.update(_job_start("span-1", pid=100))
    # no end event yet — pid must still be active
    assert stats.active_cores == 1


def test_active_cores_decrements_on_job_complete():
    "Proves a JOB_COMPLETE event removes the pid from the active set"
    stats = SystemStats()
    stats.update(_job_start("span-1", pid=100))
    stats.update(_job_end("span-1", pid=100))
    assert stats.active_cores == 0


def test_active_cores_decrements_on_job_fail():
    "Proves a JOB_FAIL event removes the pid from the active set"
    stats = SystemStats()
    stats.update(_job_start("span-1", pid=100))
    stats.update(_job_fail("span-1", pid=100))
    assert stats.active_cores == 0


def test_active_cores_counts_unique_pids():
    "Proves two start events on the same pid count as one core"
    stats = SystemStats()
    stats.update(_job_start("span-1", pid=100))
    stats.update(_job_start("span-2", pid=100))
    assert stats.active_cores == 1


def test_active_cores_counts_distinct_pids():
    "Proves job starts on distinct pids each contribute one core"
    stats = SystemStats()
    stats.update(_job_start("span-1", pid=100))
    stats.update(_job_start("span-2", pid=200))
    assert stats.active_cores == 2


def test_active_cores_resets_when_all_jobs_complete():
    "Proves active_cores returns to zero once all started jobs complete"
    stats = SystemStats()
    stats.update(_job_start("span-1", pid=100))
    stats.update(_job_start("span-2", pid=200))
    stats.update(_job_end("span-1", pid=100))
    stats.update(_job_end("span-2", pid=200))
    assert stats.active_cores == 0


def test_out_of_order_end_does_not_go_negative():
    "Proves an end event with no matching start is silently ignored"
    stats = SystemStats()
    stats.update(_job_end("span-1", pid=100))
    assert stats.active_cores == 0


def test_event_without_pid_is_ignored():
    "Proves an event missing a pid dim is silently skipped"
    stats = SystemStats()
    stats.update(point({"tag": [JobTag.ENQUEUE], "phase": [Phase.START], "id": ["span-1"], "fn": ["job"]}, at=time.time()))
    assert stats.active_cores == 0


def test_cpu_and_ram_are_zero_before_poll():
    "Proves cpu and ram default to zero before any poll"
    stats = SystemStats()
    assert stats.cpu_percent == 0.0
    assert stats.ram_percent == 0.0


def test_poll_populates_cpu_and_ram():
    "Proves poll() samples non-negative cpu and ram values from psutil"
    stats = SystemStats()
    stats.poll()
    assert stats.cpu_percent >= 0.0
    assert 0.0 < stats.ram_percent <= 100.0


def test_format_contains_cores_cpu_ram():
    "Proves format() output includes cores, cpu, and ram labels"
    from zahir.progress_bar.descriptions_view import system_description

    stats = SystemStats()
    stats.poll()
    stats.update(_job_start("span-1", pid=42))
    result = system_description(stats.active_cores, stats.cpu_percent, stats.ram_percent)
    assert "cores" in result
    assert "cpu" in result
    assert "ram" in result
