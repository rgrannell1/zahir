# Coordinates state and system-stats models; exposes their data for display
import time

from bookman.events import Event

from zahir.progress_bar.progress_bar_state_model import JobStats, ProgressBarState
from zahir.progress_bar.system_stats_service import SystemStats


def format_ms(ms: float) -> str:
    total_seconds = int(ms / 1000)
    hours, remainder = divmod(total_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}"


def eta_remaining_ms(completed: int, remaining: int, elapsed_ms: float) -> float | None:
    """Extrapolate remaining time from the observed completion rate."""

    if completed == 0 or elapsed_ms <= 0:
        return None
    rate = completed / elapsed_ms
    return remaining / rate


class ProgressBarService:
    """Owns the two model components and exposes their state for display."""

    def __init__(self):
        self._state = ProgressBarState()
        self._system = SystemStats()
        self._start_time = time.monotonic()

    def update(self, event: Event) -> None:
        self._state.update(event)
        self._system.update(event)

    def poll(self) -> None:
        self._system.poll()

    @property
    def jobs(self) -> dict[str, JobStats]:
        return self._state.jobs

    @property
    def active_cores(self) -> int:
        return self._system.active_cores

    @property
    def cpu_percent(self) -> float:
        return self._system.cpu_percent

    @property
    def ram_percent(self) -> float:
        return self._system.ram_percent

    def mean_duration_ms(self, fn_name: str) -> float | None:
        stats = self._state.jobs.get(fn_name)
        return stats.mean_ms if stats else None

    def waiting_deps(self, fn_name: str) -> dict[str, int]:
        return self._state.waiting_deps(fn_name)

    def job_progress(self, fn_name: str) -> tuple[float, int | None] | None:
        return self._state.job_progress(fn_name)

    @property
    def elapsed_ms(self) -> float:
        return (time.monotonic() - self._start_time) * 1000

    def format_elapsed(self) -> str:
        return format_ms(self.elapsed_ms)

    def format_eta(self, completed: int, remaining: int) -> str:
        ms = eta_remaining_ms(completed, remaining, self.elapsed_ms)
        return "--:--:--" if ms is None else format_ms(ms)
