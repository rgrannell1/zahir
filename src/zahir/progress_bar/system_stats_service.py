import time
from collections import deque

import psutil

from bookman.events import Event


class SystemStats:
    """Tracks cpu%, ram%, and active worker cores from telemetry events.

    cpu/ram are rolling 5-second averages sampled on each poll() call.
    Active cores are unique worker pids seen within the last _ACTIVE_WINDOW_S seconds.
    Effect-level spans last microseconds so in-flight tracking is too short-lived;
    recency-based tracking correctly reflects workers that are continuously emitting.
    """

    _CPU_WINDOW_S = 5.0
    # A pid is considered active if it emitted an event within this window
    _ACTIVE_WINDOW_S = 2.0

    def __init__(self):
        self._resource_history: deque[tuple[float, float, float]] = deque()
        self._pid_last_seen: dict[int, float] = {}

    def update(self, event: Event) -> None:
        """Record the last-seen time for the worker pid that emitted this event."""

        pid_str = event.dim("pid")
        if pid_str:
            self._pid_last_seen[int(pid_str)] = time.time()

    def poll(self) -> None:
        """Sample cpu% and ram% and add to the rolling window. Call periodically from the main loop."""

        now = time.time()
        cpu = psutil.cpu_percent(interval=0.0)
        ram = psutil.virtual_memory().percent
        self._resource_history.append((now, cpu, ram))
        cutoff = now - self._CPU_WINDOW_S

        while self._resource_history and self._resource_history[0][0] < cutoff:
            self._resource_history.popleft()

    @property
    def active_cores(self) -> int:
        """Unique worker pids seen within the last _ACTIVE_WINDOW_S seconds."""

        cutoff = time.time() - self._ACTIVE_WINDOW_S
        return sum(1 for last_seen in self._pid_last_seen.values() if last_seen >= cutoff)

    @property
    def cpu_percent(self) -> float:
        """Rolling average cpu% over the last 5 seconds."""

        if not self._resource_history:
            return 0.0
        return sum(cpu for _, cpu, _ in self._resource_history) / len(
            self._resource_history
        )

    @property
    def ram_percent(self) -> float:
        """Rolling average ram% over the last 5 seconds."""

        if not self._resource_history:
            return 0.0
        return sum(ram for _, _, ram in self._resource_history) / len(
            self._resource_history
        )
