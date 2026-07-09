# Rich-backed display service: owns the Progress widget and per-row TaskID handles
from rich.live import Live
from rich.progress import Progress, SpinnerColumn, TaskID, TextColumn

from zahir.progress_bar.columns_view import JobBarColumn, NofMColumn
from zahir.progress_bar.descriptions_view import (
    JobDisplayContext,
    job_description,
    job_status,
    system_description,
    workflow_description,
)
from zahir.progress_bar.progress_bar_service import ProgressBarService


def _visible_total(stats) -> int:
    """Return the denominator to display for a job row with any observed activity."""

    return max(stats.total, stats.started, stats.processed)


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
        self._live = Live(
            self._progress,
            auto_refresh=False,
            screen=False,
            transient=False,
        )
        self._job_tasks: dict[str, TaskID] = {}
        self._system_task: TaskID | None = None
        self._workflow_task: TaskID | None = None

    def __enter__(self):
        """Add the system and workflow tasks to the progress widget."""

        self._live.__enter__()
        self._system_task = self._progress.add_task(
            system_description(0, 0.0, 0.0),
            total=1,
            hide_bar=True,
        )
        self._workflow_task = self._progress.add_task(
            workflow_description("00:00:00", 0, 0, "--:--:--"),
            total=0,
            hide_bar=True,
        )
        return self

    def __exit__(self, *args):
        """Remove the system and workflow tasks before exiting."""

        if self._system_task is not None:
            self._progress.remove_task(self._system_task)

        return self._live.__exit__(*args)

    def refresh(self, service: ProgressBarService) -> None:
        """Refresh all the rows with the current stats from the ProgressBarService."""

        self._refresh_job_rows(service)
        self._refresh_workflow_row(service)
        self._refresh_system_row(service)
        self._live.refresh()

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
            total = _visible_total(stats)
            if total == 0:
                continue
            task_id = self._ensure_job_task(fn_name)
            ctx = JobDisplayContext(
                mean_ms=service.mean_duration_ms(fn_name),
                waiting=service.waiting_deps(fn_name),
                progress=service.job_progress(fn_name),
            )
            self._progress.update(
                task_id,
                description=job_description(fn_name, stats, ctx),
                completed=stats.processed,
                total=total,
                status=job_status(stats),
            )

    def _refresh_workflow_row(self, service: ProgressBarService) -> None:
        """Update the workflow row with the job counts and processing stats."""

        if self._workflow_task is None:
            return

        visible = [stat for stat in service.jobs.values() if _visible_total(stat) > 0]
        total = sum(_visible_total(stat) for stat in visible)
        completed = sum(stat.processed for stat in visible)
        remaining = total - completed
        eta = service.format_eta(completed, remaining)
        elapsed = service.format_elapsed()
        desc = workflow_description(elapsed, completed, remaining, eta)
        self._progress.update(
            self._workflow_task,
            description=desc,
            completed=completed,
            total=total,
        )

    def show_error(self, source: str, msg: str) -> None:
        """Print a persistent error line below the live progress bar."""

        self._live.console.print(f"[bold red][{source}][/] {msg}")

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
