import time
from collections.abc import Generator, Iterable
from typing import Any

from rich.progress import Progress, SpinnerColumn, TaskID, TextColumn

from zahir.progress_bar.columns import JobBarColumn, NofMColumn
from zahir.progress_bar.descriptions import (
    job_description,
    job_status,
    system_description,
    workflow_description,
)
from bookman.events import Event
from zahir.progress_bar.progress_bar_state import ProgressBarState
from zahir.progress_bar.system_stats import SystemStats
from zahir.progress_bar.time_estimator import TimeEstimator

_POLL_INTERVAL_S = 1.0


def with_progress(events: Iterable[Any]) -> Generator[Any, None, None]:
    """Wrap any event iterable (e.g. evaluate(...)) and render a live progress bar.

    Telemetry events are consumed for display; all events are passed through unchanged.
    cpu/ram are polled at most once per second, independent of event rate.
    """
    bar = ZahirProgressBar()
    last_poll = time.monotonic()

    with bar:
        for event in events:
            now = time.monotonic()
            if now - last_poll >= _POLL_INTERVAL_S:
                bar.poll()
                last_poll = now

            if isinstance(event, Event):
                bar.update(event)

            yield event


def _make_progress() -> Progress:
    return Progress(
        SpinnerColumn(),
        TextColumn("{task.description}"),
        JobBarColumn(),
        NofMColumn(),
    )


class ZahirProgressBar:
    """Coordinates state, estimation, and system stats into a Rich progress display."""

    def __init__(self):
        self._state = ProgressBarState()
        self._estimator = TimeEstimator()
        self._system = SystemStats()
        self._progress = _make_progress()
        self._job_tasks: dict[str, TaskID] = {}
        self._system_task: TaskID | None = None
        self._workflow_task: TaskID | None = None

    def __enter__(self):
        self._progress.__enter__()
        self._system_task = self._progress.add_task(
            system_description(0, 0.0, 0.0),
            total=None,
            hide_bar=True,
        )
        self._workflow_task = self._progress.add_task(
            workflow_description(0, 0, "--:--:--"),
            total=0,
            hide_bar=True,
        )
        return self

    def __exit__(self, *args):
        # Remove the indeterminate system row before stopping so Rich doesn't
        # freeze several spinner frames into the terminal on exit.
        if self._system_task is not None:
            self._progress.remove_task(self._system_task)
        return self._progress.__exit__(*args)

    def poll(self) -> None:
        """Sample cpu/ram from psutil. Call periodically from the main loop."""
        self._system.poll()
        self._refresh_system_row()

    def update(self, event: ZahirTelemetryEvent) -> None:
        """Feed a telemetry event into all state components and refresh the display."""
        self._state.update(event)
        self._estimator.update(event)
        self._system.update(event)
        self._refresh_job_rows()
        self._refresh_workflow_row()
        self._refresh_system_row()

    def _refresh_system_row(self) -> None:
        if self._system_task is None:
            return
        desc = system_description(
            self._system.active_cores,
            self._system.cpu_percent,
            self._system.ram_percent,
        )
        self._progress.update(self._system_task, description=desc)

    def _refresh_job_rows(self) -> None:
        for fn_name, stats in self._state.jobs.items():
            if stats.total == 0:
                continue
            task_id = self._ensure_job_task(fn_name)
            mean_ms = self._estimator.mean_duration_ms(fn_name)
            self._progress.update(
                task_id,
                description=job_description(fn_name, stats, mean_ms),
                completed=stats.processed,
                total=stats.total,
                status=job_status(stats),
            )

    def _refresh_workflow_row(self) -> None:
        if self._workflow_task is None:
            return
        enqueued = [s for s in self._state.jobs.values() if s.total > 0]
        total = sum(s.total for s in enqueued)
        processed = sum(s.processed for s in enqueued)
        desc = workflow_description(total, processed, self._estimator.format_eta())
        self._progress.update(
            self._workflow_task, description=desc, completed=processed, total=total
        )

    def _ensure_job_task(self, fn_name: str) -> TaskID:
        if fn_name not in self._job_tasks:
            task_id = self._progress.add_task(
                f"  [blue]{fn_name}[/]: starting",
                total=0,
                status="running",
            )
            self._job_tasks[fn_name] = task_id
        return self._job_tasks[fn_name]
