# Rich-backed display service: owns the Progress widget and per-row TaskID handles
from rich.progress import Progress, SpinnerColumn, TaskID, TextColumn

from zahir.progress_bar.columns_view import JobBarColumn, NofMColumn
from zahir.progress_bar.descriptions_view import (
    job_description,
    job_status,
    system_description,
    workflow_description,
)
from zahir.progress_bar.progress_bar_service import ProgressBarService


def _make_progress() -> Progress:
    """Our task progress widget."""

    return Progress(
        SpinnerColumn(),
        TextColumn("{task.description}"),
        JobBarColumn(),
        NofMColumn(),
    )


class RichDisplayService:
    """Owns the Rich Progress widget and TaskID handles; reads state from ProgressBarService."""

    def __init__(self):
        self._progress = _make_progress()
        self._job_tasks: dict[str, TaskID] = {}
        self._system_task: TaskID | None = None
        self._workflow_task: TaskID | None = None

    def __enter__(self):
        """Add the system and workflow tasks to the progress widget."""

        self._progress.__enter__()
        self._system_task = self._progress.add_task(
            system_description(0, 0.0, 0.0),
            total=1,
            hide_bar=True,
        )
        self._workflow_task = self._progress.add_task(
            workflow_description(0, 0, "--:--:--"),
            total=0,
            hide_bar=True,
        )
        return self

    def __exit__(self, *args):
        """Remove the system and workflow tasks before exiting."""

        if self._system_task is not None:
            self._progress.remove_task(self._system_task)

        return self._progress.__exit__(*args)

    def refresh(self, service: ProgressBarService) -> None:
        """Refresh all the rows with the current stats from the ProgressBarService."""

        self._refresh_job_rows(service)
        self._refresh_workflow_row(service)
        self._refresh_system_row(service)

    def _refresh_system_row(self, service: ProgressBarService) -> None:
        """Update the system row with the current stats for the system."""

        if self._system_task is None:
            return

        desc = system_description(
            service.active_cores,
            service.cpu_percent,
            service.ram_percent,
        )

        self._progress.update(self._system_task, description=desc)

    def _refresh_job_rows(self, service: ProgressBarService) -> None:
        """Update the job rows with the current stats for each function."""

        for fn_name, stats in service.jobs.items():
            if stats.total == 0:
                continue
            task_id = self._ensure_job_task(fn_name)
            mean_ms = service.mean_duration_ms(fn_name)
            waiting = service.waiting_deps(fn_name)

            self._progress.update(
                task_id,
                description=job_description(fn_name, stats, mean_ms, waiting),
                completed=stats.processed,
                total=stats.total,
                status=job_status(stats),
            )

    def _refresh_workflow_row(self, service: ProgressBarService) -> None:
        """Update the workflow row with the total number of enqueued jobs and the number of processed jobs."""

        if self._workflow_task is None:
            return

        enqueued = [s for s in service.jobs.values() if s.total > 0]
        total = sum(s.total for s in enqueued)
        processed = sum(s.processed for s in enqueued)
        desc = workflow_description(total, processed, service.format_eta())
        self._progress.update(
            self._workflow_task, description=desc, completed=processed, total=total
        )

    def _ensure_job_task(self, fn_name: str) -> TaskID:
        """Ensure a task exists for this function name."""

        if fn_name not in self._job_tasks:
            task_id = self._progress.add_task(
                f"  [blue]{fn_name}[/]: starting",
                total=0,
                status="running",
            )
            self._job_tasks[fn_name] = task_id

        return self._job_tasks[fn_name]
