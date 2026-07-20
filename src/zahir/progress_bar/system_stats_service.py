import time
from collections import deque

import psutil
from bookman.events import Event

from zahir.core.commons.constants import JobTag, Phase

# Tags that signal a job has finished, regardless of outcome
_JOB_END_TAGS = {JobTag.JOB_COMPLETE, JobTag.JOB_FAIL}
# Tags that signal a worker has picked up a job and is actively executing it
_JOB_START_TAGS = {JobTag.EXECUTE, JobTag.ENQUEUE}


class SystemStats:
    """Tracks cpu%, ram%, and active worker cores from telemetry events.

    cpu/ram are rolling 5-second averages sampled on each poll() call.
    Active cores are worker pids that have started a job but not yet completed it —
    tracked by job lifecycle events rather than a recency window, so long-running jobs
    don't fall out of the active set between their start and end events.
    """

    _CPU_WINDOW_S = 5.0

    def __init__(self):
        self._resource_history: deque[tuple[float, float, float]] = deque()
        self._executing_pids: set[int] = set()

    def update(self, event: Event) -> None:
        """Track pids that have started a job but not yet completed it."""

        pid_str = event.dim("pid")
        if not pid_str:
            return

        tag = event.dim("tag")
        phase = event.dim("phase")
        pid = int(pid_str)

        if tag == JobTag.EXECUTE or (tag == JobTag.ENQUEUE and phase == Phase.START):
            self._executing_pids.add(pid)
        elif tag in _JOB_END_TAGS and phase == Phase.END:
            self._executing_pids.discard(pid)

    def poll(self) -> None:
        """Sample cpu% and ram% and add to the rolling window."""

        now = time.time()
        cpu = psutil.cpu_percent(interval=0.0)
        ram = psutil.virtual_memory().percent
        self._resource_history.append((now, cpu, ram))

        cpu_cutoff = now - self._CPU_WINDOW_S
        while self._resource_history and self._resource_history[0][0] < cpu_cutoff:
            self._resource_history.popleft()

    @property
    def active_cores(self) -> int:
        """Worker pids that have started a job but not yet completed it."""

        return len(self._executing_pids)

    @property
    def cpu_percent(self) -> float:
        """Rolling average cpu% over the last 5 seconds."""

        if not self._resource_history:
            return 0.0
        return sum(cpu for _, cpu, _ in self._resource_history) / len(self._resource_history)

    @property
    def ram_percent(self) -> float:
        """Rolling average ram% over the last 5 seconds."""

        if not self._resource_history:
            return 0.0
        return sum(ram for _, _, ram in self._resource_history) / len(self._resource_history)
