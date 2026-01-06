from dataclasses import dataclass, field
from rich.progress import BarColumn, Progress, SpinnerColumn, TaskID, TextColumn

from zahir.events import (
    JobCompletedEvent,
    JobEvent,
    JobIrrecoverableEvent,
    JobPausedEvent,
    JobRecoveryCompletedEvent,
    JobRecoveryStartedEvent,
    JobStartedEvent,
    WorkflowCompleteEvent,
    WorkflowStartedEvent,
    ZahirEvent,
)


@dataclass
class JobTypeStats:
    """Statistics for a specific job type."""

    running: int = 0
    completed: int = 0
    failed: int = 0
    recovering: int = 0
    paused: int = 0
    task_id: TaskID | None = None


class ZahirProgressMonitor:
    """Progress monitor tracking workflow execution with per-job-type progress bars."""

    def __init__(self):
        self.progress = Progress(
            SpinnerColumn(),
            TextColumn("{task.description}"),
            BarColumn(),
            TextColumn("[progress.percentage]{task.completed}/{task.total}"),
        )
        self.job_type_stats: dict[str, JobTypeStats] = {}
        self.job_id_to_type: dict[str, str] = {}
        self.workflow_task_id: TaskID | None = None

    def __enter__(self):
        self.progress.__enter__()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return self.progress.__exit__(exc_type, exc_val, exc_tb)

    def handle_event(self, event: ZahirEvent) -> None:
        """Handle workflow events and update progress bars."""
        match event:
            case WorkflowStartedEvent():
                self.workflow_task_id = self.progress.add_task(
                    "Workflow running",
                    total=None,
                )

            case JobEvent():
                job_type = event.job["type"]
                job_id = event.job["job_id"]
                self.job_id_to_type[job_id] = job_type
                self._ensure_job_type_task(job_type)

            case JobStartedEvent():
                job_type = self.job_id_to_type.get(event.job_id)
                if job_type:
                    stats = self.job_type_stats[job_type]
                    stats.running += 1
                    self._update_job_type_description(job_type)

            case JobCompletedEvent():
                job_type = self.job_id_to_type.get(event.job_id)
                if job_type:
                    stats = self.job_type_stats[job_type]
                    stats.running = max(0, stats.running - 1)
                    stats.completed += 1
                    self._update_job_type_progress(job_type)

            case JobRecoveryStartedEvent():
                job_type = self.job_id_to_type.get(event.job_id)
                if job_type:
                    stats = self.job_type_stats[job_type]
                    stats.recovering += 1
                    self._update_job_type_description(job_type)

            case JobRecoveryCompletedEvent():
                job_type = self.job_id_to_type.get(event.job_id)
                if job_type:
                    stats = self.job_type_stats[job_type]
                    stats.recovering = max(0, stats.recovering - 1)
                    stats.completed += 1
                    self._update_job_type_progress(job_type)

            case JobPausedEvent():
                job_type = self.job_id_to_type.get(event.job_id)
                if job_type:
                    stats = self.job_type_stats[job_type]
                    stats.paused += 1
                    self._update_job_type_description(job_type)

            case JobIrrecoverableEvent():
                job_type = self.job_id_to_type.get(event.job_id)
                if job_type:
                    stats = self.job_type_stats[job_type]
                    stats.running = max(0, stats.running - 1)
                    stats.failed += 1
                    self._update_job_type_progress(job_type)

            case WorkflowCompleteEvent():
                self._finalize_workflow()

    def _ensure_job_type_task(self, job_type: str) -> None:
        """Ensure a progress task exists for the given job type."""
        if job_type not in self.job_type_stats:
            self.job_type_stats[job_type] = JobTypeStats()
            task_id = self.progress.add_task(
                f"{job_type}: starting",
                total=None,
            )
            self.job_type_stats[job_type].task_id = task_id

    def _update_job_type_description(self, job_type: str) -> None:
        """Update the progress bar description for a specific job type."""
        stats = self.job_type_stats.get(job_type)
        if stats is None or stats.task_id is None:
            return

        parts = []
        if stats.running > 0:
            parts.append(f"{stats.running} running")
        if stats.recovering > 0:
            parts.append(f"{stats.recovering} recovering")
        if stats.paused > 0:
            parts.append(f"{stats.paused} paused")

        status = ", ".join(parts) if parts else "starting"
        self.progress.update(
            stats.task_id,
            description=f"{job_type}: {stats.completed} completed, {stats.failed} failed ({status})",
        )

    def _update_job_type_progress(self, job_type: str) -> None:
        """Update progress bar for a specific job type."""
        stats = self.job_type_stats.get(job_type)
        if stats is None or stats.task_id is None:
            return

        total_processed = stats.completed + stats.failed
        self.progress.update(
            stats.task_id,
            completed=total_processed,
            total=total_processed,
        )
        self._update_job_type_description(job_type)

    def _finalize_workflow(self) -> None:
        """Finalize all progress bars when workflow completes."""
        total_completed = sum(stats.completed for stats in self.job_type_stats.values())
        total_failed = sum(stats.failed for stats in self.job_type_stats.values())

        if self.workflow_task_id is not None:
            self.progress.update(
                self.workflow_task_id,
                description=f"✓ Workflow complete: {total_completed} completed, {total_failed} failed",
            )

        for job_type, stats in self.job_type_stats.items():
            if stats.task_id is not None:
                total_processed = stats.completed + stats.failed
                self.progress.update(
                    stats.task_id,
                    completed=total_processed,
                    total=total_processed,
                    description=f"✓ {job_type}: {stats.completed} completed, {stats.failed} failed",
                )
