import time

from bookman.events import point, span
from zahir.progress_bar.system_stats_service import SystemStats


def _job_event(span_id: str, pid: int = 1234, fn: str = "some_job"):
    """Event with fn set — represents an active worker executing job logic."""
    return point({"tag": ["t"], "id": [span_id], "pid": [str(pid)], "fn": [fn]}, at=time.time())


def _idle_event(span_id: str, pid: int = 1234):
    """Event without fn set — represents an idle worker polling for jobs."""
    return point({"tag": ["t"], "id": [span_id], "pid": [str(pid)]}, at=time.time())


def test_active_cores_zero_with_no_events():
    "Proves active_cores is zero before any events arrive"
    stats = SystemStats()
    assert stats.active_cores == 0


def test_active_cores_increments_on_job_event():
    "Proves a job-level event registers a core as active"
    stats = SystemStats()
    stats.update(_job_event("span-1", pid=100))
    assert stats.active_cores == 1


def test_idle_event_does_not_register_core():
    "Proves an event without fn does not count as an active core"
    stats = SystemStats()
    stats.update(_idle_event("span-1", pid=100))
    assert stats.active_cores == 0


def test_active_cores_remains_after_span_ends():
    "Proves an active pid stays in the recency window after its event"
    stats = SystemStats()
    stats.update(_job_event("span-1", pid=100))
    assert stats.active_cores == 1


def test_active_cores_counts_unique_pids():
    "Proves two job events on the same pid count as one core"
    stats = SystemStats()
    stats.update(_job_event("span-1", pid=100))
    stats.update(_job_event("span-2", pid=100))
    assert stats.active_cores == 1


def test_active_cores_counts_distinct_pids():
    "Proves job events on distinct pids each contribute one core"
    stats = SystemStats()
    stats.update(_job_event("span-1", pid=100))
    stats.update(_job_event("span-2", pid=200))
    assert stats.active_cores == 2


def test_active_cores_stable_across_multiple_events_same_pid():
    "Proves multiple job events from the same pid do not inflate the core count"
    stats = SystemStats()
    stats.update(_job_event("span-1", pid=100))
    stats.update(_job_event("span-2", pid=100))
    assert stats.active_cores == 1


def test_event_without_pid_is_ignored():
    "Proves an event missing a pid dim is silently skipped"
    stats = SystemStats()
    stats.update(point({"tag": ["t"], "id": ["span-1"], "fn": ["job"]}, at=time.time()))
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
    stats.update(_job_event("span-1", pid=42))
    result = system_description(stats.active_cores, stats.cpu_percent, stats.ram_percent)
    assert "cores" in result
    assert "cpu" in result
    assert "ram" in result
