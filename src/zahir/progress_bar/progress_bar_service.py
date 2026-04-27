# Coordinates state and system-stats models; exposes their data for display
from bookman.events import Event

from zahir.progress_bar.progress_bar_state_model import JobStats, ProgressBarState
from zahir.progress_bar.system_stats_service import SystemStats


def format_ms(ms: float) -> str:
    total_seconds = int(ms / 1000)
    hours, remainder = divmod(total_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}"


def eta_remaining_ms(jobs: dict[str, JobStats], mean_cores: float) -> float | None:
    """Sum (in-flight count × mean duration) across all job types, divided by mean active cores."""

    candidates = [
        (in_flight, stats.mean_ms)
        for stats in jobs.values()
        for in_flight in (max(0, stats.total - stats.processed),)
        if in_flight > 0 and stats.mean_ms is not None
    ]

    if not candidates:
        return None

    total = sum(count * mean_ms for count, mean_ms in candidates)
    return total / max(1.0, mean_cores)


class ProgressBarService:
    """Owns the two model components and exposes their state for display."""

    def __init__(self):
        self._state = ProgressBarState()
        self._system = SystemStats()

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

    def format_eta(self) -> str:
        ms = eta_remaining_ms(self._state.jobs, self._system.mean_active_cores)
        return "--:--:--" if ms is None else format_ms(ms)
