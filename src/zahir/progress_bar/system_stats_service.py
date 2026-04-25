import time
from collections import deque

import psutil

from bookman.events import Event


class SystemStats:
    """Tracks cpu%, ram%, and active worker cores from telemetry events.

    cpu/ram are rolling 5-second averages sampled on each poll() call.
    Active cores are unique worker pids seen within the last _ACTIVE_WINDOW_S seconds.
    Mean active cores are a rolling average of the active_cores snapshot taken on each poll().
    Effect-level spans last microseconds so in-flight tracking is too short-lived;
    recency-based tracking correctly reflects workers that are continuously emitting.
    """

    _CPU_WINDOW_S = 5.0
    # A pid is considered active if it emitted an event within this window
    _ACTIVE_WINDOW_S = 2.0
    # Window over which active_cores samples are averaged for ETA calculation
    _CORES_WINDOW_S = 30.0

    def __init__(self):
        self._resource_history: deque[tuple[float, float, float]] = deque()
        self._pid_last_seen: dict[int, float] = {}
        self._cores_history: deque[tuple[float, int]] = deque()

    def update(self, event: Event) -> None:
        """Record the last-seen time for worker pids executing job logic.

        Only events with fn set are counted — idle workers emit EGetJob events
        (no fn) continuously; active workers emit EEnqueue, EJobComplete, and
        EJobFail events which all carry fn_name.
        """

        pid_str = event.dim("pid")
        fn = event.dim("fn")
        if pid_str and fn:
            self._pid_last_seen[int(pid_str)] = time.time()

    def poll(self) -> None:
        """Sample cpu%, ram%, and active_cores and add to their rolling windows."""

        now = time.time()
        cpu = psutil.cpu_percent(interval=0.0)
        ram = psutil.virtual_memory().percent
        self._resource_history.append((now, cpu, ram))

        cores_cutoff = now - self._CORES_WINDOW_S
        self._cores_history.append((now, self.active_cores))
        while self._cores_history and self._cores_history[0][0] < cores_cutoff:
            self._cores_history.popleft()

        cpu_cutoff = now - self._CPU_WINDOW_S
        while self._resource_history and self._resource_history[0][0] < cpu_cutoff:
            self._resource_history.popleft()

    @property
    def mean_active_cores(self) -> float:
        """Rolling mean of active_cores snapshots over the last _CORES_WINDOW_S seconds.

        Defaults to 1.0 before any samples are recorded so ETA stays conservative.
        """
        if not self._cores_history:
            return 1.0
        return sum(count for _, count in self._cores_history) / len(self._cores_history)

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
