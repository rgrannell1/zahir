import time

from bookman.create import point, span
from zahir.progress_bar.system_stats import SystemStats


def _start(span_id: str, pid: int = 1234):
    return point({"tag": ["t"], "id": [span_id], "pid": [str(pid)]}, at=time.time())


def _end(span_id: str, pid: int = 1234):
    now = time.time()
    return span(
        {"tag": ["t"], "id": [span_id], "pid": [str(pid)]}, at=now, until=now + 0.1
    )


def test_active_cores_zero_with_no_events():
    "Proves active_cores is zero before any events arrive"
    stats = SystemStats()
    assert stats.active_cores == 0


def test_active_cores_increments_on_start():
    "Proves a start event registers a core as active"
    stats = SystemStats()
    stats.update(_start("span-1", pid=100))
    assert stats.active_cores == 1


def test_active_cores_drops_to_zero_after_span_ends():
    "Proves a completed span no longer contributes to active_cores"
    stats = SystemStats()
    stats.update(_start("span-1", pid=100))
    stats.update(_end("span-1", pid=100))
    assert stats.active_cores == 0


def test_active_cores_counts_unique_pids():
    "Proves two spans on the same pid count as one core"
    stats = SystemStats()
    stats.update(_start("span-1", pid=100))
    stats.update(_start("span-2", pid=100))
    assert stats.active_cores == 1


def test_active_cores_counts_distinct_pids():
    "Proves spans on distinct pids each contribute one core"
    stats = SystemStats()
    stats.update(_start("span-1", pid=100))
    stats.update(_start("span-2", pid=200))
    assert stats.active_cores == 2


def test_ending_one_span_keeps_pid_active_while_another_is_inflight():
    "Proves a pid with two in-flight spans remains active after one ends"
    stats = SystemStats()
    stats.update(_start("span-1", pid=100))
    stats.update(_start("span-2", pid=100))
    stats.update(_end("span-1", pid=100))
    assert stats.active_cores == 1


def test_ending_all_spans_for_pid_removes_it():
    "Proves a pid drops from active_cores once all its spans have ended"
    stats = SystemStats()
    stats.update(_start("span-1", pid=100))
    stats.update(_start("span-2", pid=100))
    stats.update(_end("span-1", pid=100))
    stats.update(_end("span-2", pid=100))
    assert stats.active_cores == 0


def test_end_event_with_no_matching_start_is_ignored():
    "Proves a stray end event does not cause errors or phantom cores"
    stats = SystemStats()
    stats.update(_end("span-unknown", pid=100))
    assert stats.active_cores == 0


def test_event_without_pid_is_ignored():
    "Proves a start event missing a pid dim is silently skipped"
    stats = SystemStats()
    stats.update(point({"tag": ["t"], "id": ["span-1"]}, at=time.time()))
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
    stats = SystemStats()
    stats.poll()
    stats.update(_start("span-1", pid=42))
    result = stats.format()
    assert "cores" in result
    assert "cpu" in result
    assert "ram" in result
