import time
from collections import deque

import psutil

from bookman.events import Event

from zahir.core.constants import JobTag, Phase

# Tags that signal a job has finished, regardless of outcome
_JOB_END_TAGS = {JobTag.JOB_COMPLETE, JobTag.JOB_FAIL}


class SystemStats:
    """Tracks cpu%, ram%, and active worker cores from telemetry events.

    cpu/ram are rolling 5-second averages sampled on each poll() call.
    Active cores are worker pids that have started a job but not yet completed it —
    tracked by job lifecycle events rather than a recency window, so long-running jobs
    don't fall out of the active set between their start and end events.
    Mean active cores are a rolling average of the active_cores snapshot taken on each poll().
    """

    _CPU_WINDOW_S = 5.0
    # Window over which active_cores samples are averaged for ETA calculation
    _CORES_WINDOW_S = 30.0

    def __init__(self):
        self._resource_history: deque[tuple[float, float, float]] = deque()
        self._executing_pids: set[int] = set()
        self._cores_history: deque[tuple[float, int]] = deque()

    def update(self, event: Event) -> None:
        """Track pids that have started a job but not yet completed it."""

        pid_str = event.dim("pid")
        if not pid_str:
            return

        tag = event.dim("tag")
        phase = event.dim("phase")
        pid = int(pid_str)

        if tag == JobTag.ENQUEUE and phase == Phase.START:
            self._executing_pids.add(pid)
        elif tag in _JOB_END_TAGS and phase == Phase.END:
            self._executing_pids.discard(pid)

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
        """Worker pids that have started a job but not yet completed it."""

        return len(self._executing_pids)

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
