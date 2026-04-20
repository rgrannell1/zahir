import time

from zahir.progress_bar.events import ZahirSpanEnd, ZahirTelemetryEvent
from zahir.progress_bar.system_stats import SystemStats


def _start(pid=1234):
    return ZahirTelemetryEvent(
        span_id="s",
        tag="t",
        event="start",
        timestamp=time.time(),
        attributes={"fn_name": "job_a", "pid": pid},
    )


def _end(pid=1234):
    return ZahirSpanEnd(
        span_id="s",
        tag="t",
        event="end",
        timestamp=time.time(),
        attributes={"fn_name": "job_a", "pid": pid},
        duration_ms=100.0,
    )


def test_active_cores_zero_with_no_events():
    stats = SystemStats()
    assert stats.active_cores == 0


def test_active_cores_counts_unique_pids_from_start_events():
    stats = SystemStats()
    stats.update(_start(pid=100))
    stats.update(_start(pid=200))
    stats.update(_start(pid=100))  # duplicate pid — should count once
    assert stats.active_cores == 2


def test_end_events_do_not_increment_active_cores():
    stats = SystemStats()
    stats.update(_end(pid=100))
    assert stats.active_cores == 0


def test_event_without_pid_is_ignored():
    stats = SystemStats()
    event = ZahirTelemetryEvent(
        span_id="s", tag="t", event="start", timestamp=time.time(), attributes={}
    )
    stats.update(event)
    assert stats.active_cores == 0


def test_cpu_and_ram_are_zero_before_poll():
    stats = SystemStats()
    assert stats.cpu_percent == 0.0
    assert stats.ram_percent == 0.0


def test_poll_populates_cpu_and_ram():
    stats = SystemStats()
    stats.poll()
    # psutil returns non-negative values; 0.0 is valid on idle systems
    assert stats.cpu_percent >= 0.0
    assert 0.0 < stats.ram_percent <= 100.0


def test_format_contains_cores_cpu_ram():
    stats = SystemStats()
    stats.poll()
    stats.update(_start(pid=42))
    result = stats.format()
    assert "cores" in result
    assert "cpu" in result
    assert "ram" in result


def test_pid_window_evicts_old_events():
    stats = SystemStats()
    # Manually inject an old event by bypassing update
    old_time = time.time() - (SystemStats._PID_WINDOW_S + 1)
    stats._pid_events.append((old_time, 999))
    # Trigger eviction via active_cores
    assert stats.active_cores == 0
