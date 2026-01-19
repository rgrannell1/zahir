from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
import time
from typing import Protocol

import psutil
from rich.bar import Bar
from rich.progress import (
    Progress,
    ProgressColumn,
    SpinnerColumn,
    TaskID,
    TextColumn,
)
from rich.text import Text

from zahir.events import (
    JobCompletedEvent,
    JobEvent,
    JobIrrecoverableEvent,
    JobPausedEvent,
    JobImpossibleEvent,
    JobRecoveryCompletedEvent,
    JobRecoveryStartedEvent,
    JobStartedEvent,
    WorkflowCompleteEvent,
    WorkflowStartedEvent,
    ZahirEvent,
)


def format_job_type_blue(job_type: str) -> str:
    """Wrap job type in blue Rich markup without affecting width."""
    return f"[blue]{job_type}[/]"


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
    durations: list[float] = field(default_factory=list)  # Track durations for average calculation




class ConditionalSpinnerColumn(SpinnerColumn):
    """Spinner column that can be conditionally hidden based on task description."""

    def __init__(self, hide_if_starts_with: str | None = None):
        super().__init__()
        self.hide_if_starts_with = hide_if_starts_with

    def render(self, task) -> Text:
        if self.hide_if_starts_with and task.description and task.description.startswith(self.hide_if_starts_with):
            return Text("")  # Hide for system stats
        return super().render(task)


class ConditionalTextColumn(TextColumn):
    """Text column that can be conditionally hidden based on task description."""

    def __init__(self, text_format: str, hide_if_starts_with: str | None = None):
        super().__init__(text_format)
        self.hide_if_starts_with = hide_if_starts_with

    def render(self, task) -> Text:
        if self.hide_if_starts_with and task.description and task.description.startswith(self.hide_if_starts_with):
            return Text("")  # Hide for system stats
        return super().render(task)


class StatusBarColumn(ProgressColumn):
    """Progress bar column whose color depends on task.fields['status'].

    Supported statuses:
      - "running" (default): in-flight
      - "success": finished with zero failures
      - "failed": finished with >= 1 failure (or any failure, depending on how you set it)
    """

    def __init__(
        self,
        width: int | None = 30,
        running_complete_style: str = "cyan",
        success_complete_style: str = "green",
        failed_complete_style: str = "red",
        back_style: str = "grey37",
    ) -> None:
        super().__init__()
        self.width = width
        self.running_complete_style = running_complete_style
        self.success_complete_style = success_complete_style
        self.failed_complete_style = failed_complete_style
        self.back_style = back_style

    def render(self, task) -> Text:
        # Hide bar for system stats task (identified by description starting with "Zahir |")
        if task.description and task.description.startswith("Zahir |"):
            return Text("")  # Empty text, no bar

        status = task.fields.get("status", "running")

        if status == "failed":
            complete_style = self.failed_complete_style
        elif status == "success":
            complete_style = self.success_complete_style
        else:
            complete_style = self.running_complete_style

        bar = Bar(
            size=task.total or 1,
            begin=0,
            end=task.completed,
            width=self.width,
            color=complete_style,
            bgcolor=self.back_style,
        )
        return bar


class ZahirProgressMonitor:
    """Progress monitor tracking workflow execution with per-job-type progress bars."""

    def __init__(self):
        self.progress = Progress(
            ConditionalSpinnerColumn(hide_if_starts_with="Zahir |"),
            TextColumn("{task.description}"),
            StatusBarColumn(width=30),
            ConditionalTextColumn(
                "[progress.percentage]{task.completed}/{task.total}",
                hide_if_starts_with="Zahir |"
            ),
        )
        self.job_type_stats: dict[str, JobTypeStats] = {}
        self.job_id_to_type: dict[str, str] = {}
        # Track active state of each job: "running", "paused", or "recovering"
        self.job_id_to_active_state: dict[str, str] = {}
        self.workflow_task_id: TaskID | None = None
        self.system_stats_task_id: TaskID | None = None
        self.impossible_jobs_count: int = 0
        # Track (timestamp, pid) tuples for active processes in a 3s window
        self.pid_events: deque[tuple[float, int]] = deque()
        # Track (timestamp, cpu_percent, ram_percent) tuples for 5-second window
        self.system_stats_history: deque[tuple[float, float, float]] = deque()

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

        # Update system stats on every event (after PID tracking for accurate core count)
        self._update_system_stats()

        match event:
            case WorkflowStartedEvent():
                # Add system stats task at the top
                self.system_stats_task_id = self.progress.add_task(
                    "Zahir | 0 cores | cpu 0% ram 0%",
                    total=1,
                    completed=0,
                    status="running",
                )
                self.workflow_task_id = self.progress.add_task(
                    "Workflow running",
                    total=0,
                    completed=0,
                    status="running",
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

                # Update description to reflect current state
                self._update_job_type_description(job_type)

                # Update workflow total + status
                self._update_workflow_progress()

            case JobStartedEvent():
                job_type = self.job_id_to_type.get(event.job_id)
                if job_type:
                    stats = self.job_type_stats[job_type]
                    # If job was previously paused, decrement paused count
                    if self.job_id_to_active_state.get(event.job_id) == "paused":
                        stats.paused = max(0, stats.paused - 1)
                    stats.running += 1
                    self.job_id_to_active_state[event.job_id] = "running"
                    self._update_job_type_description(job_type)

            case JobCompletedEvent():
                job_type = self.job_id_to_type.get(event.job_id)
                if job_type:
                    stats = self.job_type_stats[job_type]
                    # Decrement the correct counter based on job's active state
                    active_state = self.job_id_to_active_state.pop(event.job_id, "running")
                    if active_state == "paused":
                        stats.paused = max(0, stats.paused - 1)
                    elif active_state == "recovering":
                        stats.recovering = max(0, stats.recovering - 1)
                    else:  # running or unknown
                        stats.running = max(0, stats.running - 1)
                    stats.completed += 1
                    # Track duration for average calculation
                    stats.durations.append(event.duration_seconds)
                    self._update_job_type_progress(job_type)
                    # Update workflow description to refresh ETA
                    self._update_workflow_description()

            case JobRecoveryStartedEvent():
                job_type = self.job_id_to_type.get(event.job_id)
                if job_type:
                    stats = self.job_type_stats[job_type]
                    # If job was previously running, decrement running count
                    if self.job_id_to_active_state.get(event.job_id) == "running":
                        stats.running = max(0, stats.running - 1)
                    stats.recovering += 1
                    self.job_id_to_active_state[event.job_id] = "recovering"
                    self._update_job_type_description(job_type)

            case JobRecoveryCompletedEvent():
                job_type = self.job_id_to_type.get(event.job_id)
                if job_type:
                    stats = self.job_type_stats[job_type]
                    stats.recovering = max(0, stats.recovering - 1)
                    self.job_id_to_active_state.pop(event.job_id, None)
                    stats.completed += 1
                    # Track duration for average calculation
                    stats.durations.append(event.duration_seconds)
                    self._update_job_type_progress(job_type)
                    # Update workflow description to refresh ETA
                    self._update_workflow_description()

            case JobPausedEvent():
                job_type = self.job_id_to_type.get(event.job_id)
                if job_type:
                    stats = self.job_type_stats[job_type]
                    # If job was previously running, decrement running count
                    if self.job_id_to_active_state.get(event.job_id) == "running":
                        stats.running = max(0, stats.running - 1)
                    stats.paused += 1
                    self.job_id_to_active_state[event.job_id] = "paused"
                    self._update_job_type_description(job_type)

            case JobIrrecoverableEvent():
                job_type = self.job_id_to_type.get(event.job_id)
                if job_type:
                    stats = self.job_type_stats[job_type]
                    # Decrement the correct counter based on job's active state
                    active_state = self.job_id_to_active_state.pop(event.job_id, "running")
                    if active_state == "paused":
                        stats.paused = max(0, stats.paused - 1)
                    elif active_state == "recovering":
                        stats.recovering = max(0, stats.recovering - 1)
                    else:  # running or unknown
                        stats.running = max(0, stats.running - 1)
                    stats.failed += 1
                    self._update_job_type_progress(job_type)
                    # Update workflow description to refresh ETA
                    self._update_workflow_description()

            case JobImpossibleEvent():
                self.impossible_jobs_count += 1
                self._update_workflow_description()

            case WorkflowCompleteEvent():
                self._finalize_workflow()

    def _update_system_stats(self) -> None:
        """Update system stats display with 5-second averages and core count."""
        if self.system_stats_task_id is None:
            return

        current_time = time.time()

        # Add current stats
        cpu_percent = psutil.cpu_percent(interval=0.0)  # Non-blocking
        ram_percent = psutil.virtual_memory().percent
        self.system_stats_history.append((current_time, cpu_percent, ram_percent))

        # Remove stats older than 5 seconds
        cutoff_time = current_time - 5.0
        while self.system_stats_history and self.system_stats_history[0][0] < cutoff_time:
            self.system_stats_history.popleft()

        # Calculate averages
        if self.system_stats_history:
            avg_cpu = sum(cpu for _, cpu, _ in self.system_stats_history) / len(self.system_stats_history)
            avg_ram = sum(ram for _, _, ram in self.system_stats_history) / len(self.system_stats_history)
        else:
            avg_cpu = cpu_percent
            avg_ram = ram_percent

        # Get active process count (cores)
        active_processes = self._get_active_process_count()

        self.progress.update(
            self.system_stats_task_id,
            description=f"Zahir | {active_processes} cores | cpu {avg_cpu:.0f}% ram {avg_ram:.0f}%",
        )

    def _cleanup_old_pids(self, current_time: float) -> None:
        """Remove PID events older than 3 seconds."""
        cutoff_time = current_time - 3.0
        while self.pid_events and self.pid_events[0][0] < cutoff_time:
            self.pid_events.popleft()

    def _get_active_process_count(self) -> int:
        """Get count of unique processes that emitted events in the last 3 seconds."""
        return len(set(pid for _, pid in self.pid_events))

    def _calculate_eta_seconds(self) -> float | None:
        total_eta_seconds = 0.0
        has_data = False

        for stats in self.job_type_stats.values():
            remaining = stats.total - stats.completed - stats.failed

            if remaining <= 0:
                continue

            if stats.durations:
                # estimate remaining time based on average duration
                avg_duration = sum(stats.durations) / len(stats.durations)
                total_eta_seconds += avg_duration * remaining
                has_data = True

        return total_eta_seconds if has_data else None

    def _format_eta_minutes(self, eta_seconds: float | None) -> str:
        """Format ETA in seconds to a human-readable minutes string."""
        if eta_seconds is None:
            return ""

        eta_minutes = eta_seconds / 60.0

        if eta_minutes < 0.1:
            return f"eta <1min"
        elif eta_minutes < 1.0:
            return f"eta {eta_minutes*60:.0f}s"
        elif eta_minutes < 10.0:
            return f"eta {eta_minutes:.1f}min"
        else:
            return f"eta {eta_minutes:.0f}min"

    def _update_workflow_description(self) -> None:
        if self.workflow_task_id is None:
            return

        desc_parts = ["Workflow running"]
        if self.impossible_jobs_count > 0:
            desc_parts.append(f"{self.impossible_jobs_count} impossible")

        eta_seconds = self._calculate_eta_seconds()
        eta_text = self._format_eta_minutes(eta_seconds)
        if eta_text:
            desc_parts.append(eta_text)

        self.progress.update(
            self.workflow_task_id,
            description=" ".join(desc_parts),
        )

    def _ensure_job_type_task(self, job_type: str) -> None:
        """Ensure a progress task exists for the given job type."""
        if job_type not in self.job_type_stats:
            stats = JobTypeStats()
            self.job_type_stats[job_type] = stats
            task_id = self.progress.add_task(
                f"  {format_job_type_blue(job_type)}: starting",
                total=0,
                completed=0,
                status="running",
            )
            stats.task_id = task_id

    def _set_job_type_status(self, job_type: str) -> None:
        """Set per-job-type task status to drive bar colour."""
        stats = self.job_type_stats.get(job_type)
        if stats is None or stats.task_id is None:
            return

        processed = stats.completed + stats.failed

        # Policy:
        # - If any failures occurred, keep it "failed" (red), even if some succeeded.
        # - Else if fully processed, "success" (green).
        # - Else "running" (cyan).
        if stats.failed > 0:
            status = "failed"
        elif stats.total > 0 and processed >= stats.total:
            status = "success"
        else:
            status = "running"

        self.progress.update(stats.task_id, status=status)

    def _set_workflow_status(self) -> None:
        """Set workflow task status to drive bar colour."""
        if self.workflow_task_id is None:
            return

        total_jobs = sum(stats.total for stats in self.job_type_stats.values())
        total_failed = sum(stats.failed for stats in self.job_type_stats.values())
        total_processed = sum((stats.completed + stats.failed) for stats in self.job_type_stats.values())

        if total_jobs > 0 and total_processed >= total_jobs:
            status = "failed" if total_failed > 0 else "success"
        else:
            status = "running"

        self.progress.update(self.workflow_task_id, status=status)

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

        # Build description parts
        desc_parts = []
        if parts:
            desc_parts.append(", ".join(parts))

        # Always show completed/failed counts
        if stats.completed > 0 or stats.failed > 0:
            status_text = f"{stats.completed} completed"
            if stats.failed > 0:
                status_text += f", {stats.failed} failed"
            desc_parts.append(status_text)
        elif not parts:
            # No active jobs and no completed jobs yet - show "starting"
            desc_parts.append("starting")

        avg_duration_text = ""
        if stats.durations:
            avg_duration = sum(stats.durations) / len(stats.durations)
            # Format: μ<seconds> with appropriate precision
            if avg_duration < 0.1:
                avg_duration_text = f" μ{avg_duration*1000:.0f}ms"
            elif avg_duration < 1.0:
                avg_duration_text = f" μ{avg_duration:.2f}s"
            elif avg_duration < 10.0:
                avg_duration_text = f" μ{avg_duration:.1f}s"
            else:
                avg_duration_text = f" μ{avg_duration:.0f}s"

        description = f"  {format_job_type_blue(job_type)}: {', '.join(desc_parts)}{avg_duration_text}"
        self.progress.update(
            stats.task_id,
            description=description,
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
            total=stats.total,
        )
        self._update_job_type_description(job_type)
        self._set_job_type_status(job_type)
        self._update_workflow_progress()

    def _update_workflow_progress(self) -> None:
        """Update workflow progress bar with current totals."""
        if self.workflow_task_id is None:
            return

        total_completed = sum(stats.completed for stats in self.job_type_stats.values())
        total_failed = sum(stats.failed for stats in self.job_type_stats.values())
        total_processed = total_completed + total_failed
        total_jobs = sum(stats.total for stats in self.job_type_stats.values())

        self.progress.update(
            self.workflow_task_id,
            completed=total_processed,
            total=total_jobs,
        )
        self._set_workflow_status()

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
                status="failed" if total_failed > 0 else "success",
                description=f"✓ Workflow complete: {total_completed} completed, {total_failed} failed",
            )

        for job_type, stats in self.job_type_stats.items():
            if stats.task_id is not None:
                job_total_processed = stats.completed + stats.failed
                self.progress.update(
                    stats.task_id,
                    completed=job_total_processed,
                    total=stats.total,
                    status="failed" if stats.failed > 0 else "success",
                    description=f"  ✓ {format_job_type_blue(job_type)}: {stats.completed} completed, {stats.failed} failed",
                )
