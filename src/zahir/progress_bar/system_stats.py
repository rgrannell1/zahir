import time
from collections import deque

import psutil

from bookman.events import Event


class SystemStats:
    """Tracks cpu%, ram%, and active worker cores from telemetry events.

    cpu/ram are rolling 5-second averages sampled on each poll() call.
    Active cores are the number of unique worker pids with at least one
    in-flight span — a span is in-flight from its start point until its
    end span arrives.
    """

    _CPU_WINDOW_S = 5.0

    def __init__(self):
        self._resource_history: deque[tuple[float, float, float]] = deque()
        self._inflight: dict[str, int] = {}

    def update(self, event: Event) -> None:
        """Track in-flight spans to derive active core count."""
        if event.kind == "span":
            self._inflight.pop(event.dim("id"), None)
            return

        pid_str = event.dim("pid")
        if pid_str:
            self._inflight[event.dim("id")] = int(pid_str)

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
        """Unique worker pids with at least one in-flight span."""
        return len(set(self._inflight.values()))

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

    def format(self) -> str:
        """Format the current system stats for display in the progress bar. TODO move into progress bar"""
        return f"{self.active_cores} cores | cpu {self.cpu_percent:.0f}% ram {self.ram_percent:.0f}%"
