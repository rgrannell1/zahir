from collections import deque
from dataclasses import dataclass
import time
from typing import Protocol

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


class ProgressMonitor(Protocol):
    """Protocol for progress monitors to support dependency injection."""

    def handle_event(self, event: ZahirEvent) -> None:
        """Handle workflow events and update progress tracking."""
        ...

    def __enter__(self) -> "ProgressMonitor":
        """Context manager entry."""
        ...

    def __exit__(self, exc_type: type | None, exc_val: Exception | None, exc_tb: object | None) -> bool | None:
        """Context manager exit."""
        ...


class NoOpProgressMonitor:
    """A progress monitor that does nothing. Use to disable progress output."""

    def handle_event(self, event: ZahirEvent) -> None:
        pass

    def __enter__(self) -> "NoOpProgressMonitor":
        return self

    def __exit__(self, exc_type: type | None, exc_val: Exception | None, exc_tb: object | None) -> bool | None:
        return None


@dataclass
class JobTypeStats:
    """Statistics for a specific job type."""

    running: int = 0
    completed: int = 0
    failed: int = 0
    recovering: int = 0
    paused: int = 0
    total: int = 0  # Total number of jobs of this type
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
        # Track (timestamp, pid) tuples for active processes in a 3s window
        self.pid_events: deque[tuple[float, int]] = deque()

    def __enter__(self):
        self.progress.__enter__()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return self.progress.__exit__(exc_type, exc_val, exc_tb)

    def handle_event(self, event: ZahirEvent) -> None:
        """Handle workflow events and update progress bars."""
        # Track PID from event if available
        if hasattr(event, "pid"):
            current_time = time.time()
            self.pid_events.append((current_time, event.pid))
            self._cleanup_old_pids(current_time)
            self._update_workflow_description()

        match event:
            case WorkflowStartedEvent():
                self.workflow_task_id = self.progress.add_task(
                    "Workflow running",
                    total=0,
                    completed=0,
                )

            case JobEvent():
                job_type = event.job["type"]
                job_id = event.job["job_id"]
                self.job_id_to_type[job_id] = job_type
                self._ensure_job_type_task(job_type)
                stats = self.job_type_stats[job_type]
                stats.total += 1
                # Update the progress bar total
                if stats.task_id is not None:
                    self.progress.update(
                        stats.task_id,
                        total=stats.total,
                    )
                # Update workflow total
                self._update_workflow_progress()

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

    def _cleanup_old_pids(self, current_time: float) -> None:
        """Remove PID events older than 3 seconds."""
        cutoff_time = current_time - 3.0
        while self.pid_events and self.pid_events[0][0] < cutoff_time:
            self.pid_events.popleft()

    def _get_active_process_count(self) -> int:
        """Get count of unique processes that emitted events in the last 3 seconds."""
        return len(set(pid for _, pid in self.pid_events))

    def _update_workflow_description(self) -> None:
        """Update workflow task description with active process count."""
        if self.workflow_task_id is None:
            return

        active_processes = self._get_active_process_count()
        total_completed = sum(stats.completed for stats in self.job_type_stats.values())
        total_failed = sum(stats.failed for stats in self.job_type_stats.values())

        self.progress.update(
            self.workflow_task_id,
            description=f"Workflow running ({active_processes} cores)",
        )

    def _ensure_job_type_task(self, job_type: str) -> None:
        """Ensure a progress task exists for the given job type."""
        if job_type not in self.job_type_stats:
            stats = JobTypeStats()
            self.job_type_stats[job_type] = stats
            task_id = self.progress.add_task(
                f"  {job_type}: starting",
                total=0,
                completed=0,
            )
            stats.task_id = task_id

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

        self.progress.update(
            stats.task_id,
            description=f"  {job_type}: {stats.completed} completed, {stats.failed} failed",
        )

    def _update_job_type_progress(self, job_type: str) -> None:
        """Update progress bar for a specific job type."""
        stats = self.job_type_stats.get(job_type)
        if stats is None or stats.task_id is None:
            return

        total_processed = stats.completed + stats.failed
        # Use the actual total, not just processed count
        self.progress.update(
            stats.task_id,
            completed=total_processed,
            total=stats.total,
        )
        self._update_job_type_description(job_type)
        self._update_workflow_progress()

    def _update_workflow_progress(self) -> None:
        """Update workflow progress bar with current totals."""
        if self.workflow_task_id is None:
            return

        total_completed = sum(stats.completed for stats in self.job_type_stats.values())
        total_failed = sum(stats.failed for stats in self.job_type_stats.values())
        total_processed = total_completed + total_failed
        # Use the actual total across all job types
        total_jobs = sum(stats.total for stats in self.job_type_stats.values())

        self.progress.update(
            self.workflow_task_id,
            completed=total_processed,
            total=total_jobs,
        )

    def _finalize_workflow(self) -> None:
        """Finalize all progress bars when workflow completes."""
        total_completed = sum(stats.completed for stats in self.job_type_stats.values())
        total_failed = sum(stats.failed for stats in self.job_type_stats.values())
        total_processed = total_completed + total_failed
        total_jobs = sum(stats.total for stats in self.job_type_stats.values())

        if self.workflow_task_id is not None:
            self.progress.update(
                self.workflow_task_id,
                completed=total_processed,
                total=total_jobs,
                description=f"✓ Workflow complete: {total_completed} completed, {total_failed} failed",
            )

        for job_type, stats in self.job_type_stats.items():
            if stats.task_id is not None:
                job_total_processed = stats.completed + stats.failed
                self.progress.update(
                    stats.task_id,
                    completed=job_total_processed,
                    total=stats.total,
                    description=f"  ✓ {job_type}: {stats.completed} completed, {stats.failed} failed",
                )
