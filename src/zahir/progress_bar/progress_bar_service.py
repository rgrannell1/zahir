# Coordinates state, timing, and system-stats models; exposes their data for display
from bookman.events import Event

from zahir.progress_bar.progress_bar_state_model import JobStats, ProgressBarState
from zahir.progress_bar.system_stats_service import SystemStats
from zahir.progress_bar.time_estimator_service import TimeEstimator


class ProgressBarService:
    """Owns the three model components and exposes their state for display."""

    def __init__(self):
        self._state = ProgressBarState()
        self._estimator = TimeEstimator()
        self._system = SystemStats()

    def update(self, event: Event) -> None:
        self._state.update(event)
        self._estimator.update(event)
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
        return self._estimator.mean_duration_ms(fn_name)

    def format_eta(self) -> str:
        return self._estimator.format_eta()
