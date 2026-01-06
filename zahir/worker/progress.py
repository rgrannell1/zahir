from rich.progress import BarColumn, Progress, SpinnerColumn, TaskID, TextColumn

from zahir.events import (
    JobCompletedEvent,
    JobIrrecoverableEvent,
    JobPausedEvent,
    JobRecoveryCompletedEvent,
    JobRecoveryStartedEvent,
    JobStartedEvent,
    WorkflowCompleteEvent,
    WorkflowStartedEvent,
    ZahirEvent,
)


class ZahirProgressMonitor:
    """Flat progress bar for tracking workflow execution across thousands of jobs."""

    def __init__(self):
        self.progress = Progress(
            SpinnerColumn(),
            TextColumn("{task.description}"),
            BarColumn(),
            TextColumn("[progress.percentage]{task.completed}/{task.total}"),
        )
        self.task_id: TaskID | None = None
        self.running = 0
        self.completed = 0
        self.failed = 0
        self.recovering = 0
        self.paused = 0

    def __enter__(self):
        self.progress.__enter__()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return self.progress.__exit__(exc_type, exc_val, exc_tb)

    def handle_event(self, event: ZahirEvent) -> None:
        """Handle workflow events and update progress bar."""
        match event:
            case WorkflowStartedEvent():
                # Start with indeterminate progress
                self.task_id = self.progress.add_task(
                    "Workflow running",
                    total=None,
                )

            case JobStartedEvent():
                self.running += 1
                self._update_description()

            case JobCompletedEvent():
                self.running = max(0, self.running - 1)
                self.completed += 1
                self._update_progress()

            case JobRecoveryStartedEvent():
                self.recovering += 1
                self._update_description()

            case JobRecoveryCompletedEvent():
                self.recovering = max(0, self.recovering - 1)
                self.completed += 1
                self._update_progress()

            case JobPausedEvent():
                self.paused += 1
                self._update_description()

            case JobIrrecoverableEvent():
                self.running = max(0, self.running - 1)
                self.failed += 1
                self._update_progress()

            case WorkflowCompleteEvent():
                if self.task_id is not None:
                    self.progress.update(
                        self.task_id,
                        description=f"✓ Workflow complete: {self.completed} completed, {self.failed} failed",
                    )

    def _update_description(self) -> None:
        """Update the progress bar description with current stats."""
        if self.task_id is None:
            return

        parts = []
        if self.running > 0:
            parts.append(f"{self.running} running")
        if self.recovering > 0:
            parts.append(f"{self.recovering} recovering")
        if self.paused > 0:
            parts.append(f"{self.paused} paused")

        status = ", ".join(parts) if parts else "starting"
        self.progress.update(
            self.task_id,
            description=f"Workflow: {self.completed} completed, {self.failed} failed ({status})",
        )

    def _update_progress(self) -> None:
        """Update progress bar with completed jobs."""
        if self.task_id is None:
            return

        total_processed = self.completed + self.failed
        self.progress.update(
            self.task_id,
            completed=total_processed,
            total=total_processed,  # Keep extending as we go
        )
        self._update_description()
