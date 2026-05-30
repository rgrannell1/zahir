from dataclasses import dataclass
from typing import Any

from bookman.events import Event

from zahir.core.constants import DependencyTag, JobTag, Phase
from zahir.progress_bar.metrics import per_fn_progress_agg


@dataclass
class JobStats:
    total: int = 0
    started: int = 0
    completed: int = 0
    failed: int = 0
    mean_ms: float | None = None

    @property
    def processed(self) -> int:
        return self.completed + self.failed


def record_pid_mapping(pid_fn: dict[int, str], pid: int, fn_name: str, event: Event) -> None:
    """Update the pid→fn_name map when a worker starts a new job."""
    tag = event.dim("tag")
    phase = event.dim("phase")
    if tag == JobTag.EXECUTE:
        # job:execute fires in the executing worker — the authoritative pid→fn mapping
        pid_fn[pid] = fn_name
    elif tag == JobTag.ENQUEUE and phase == Phase.START:
        # fallback: enqueuer's pid, used when execute event is not present
        pid_fn[pid] = fn_name


def record_waiting_state(pid_waiting: dict[int, str], pid: int, tag: str, dep: str | None) -> None:
    """Update the pid→dep map as dependency state changes."""
    if tag == DependencyTag.WAITING and dep:
        pid_waiting[pid] = dep
    elif tag in {DependencyTag.SATISFIED, JobTag.JOB_COMPLETE, JobTag.JOB_FAIL, JobTag.ENQUEUE}:
        pid_waiting.pop(pid, None)


def extract_job_stats(agg, acc: Any) -> JobStats:
    """Extract a JobStats from a per_fn_progress_agg accumulator."""
    (total, started, completed, failed), mean_ms = agg.extract(acc)
    return JobStats(
        total=total,
        started=started,
        completed=completed,
        failed=failed,
        mean_ms=mean_ms,
    )


class ProgressBarState:
    def __init__(self):
        self._agg = per_fn_progress_agg()
        self._acc: dict[str, Any] = {}
        self._pid_fn: dict[int, str] = {}
        self._pid_waiting: dict[int, str] = {}

    @property
    def jobs(self) -> dict[str, JobStats]:
        return {fn: extract_job_stats(self._agg, acc) for fn, acc in self._acc.items()}

    def waiting_deps(self, fn_name: str) -> dict[str, int]:
        """Return {dep_label: count} for jobs of this fn_name currently blocked on a dependency."""
        result: dict[str, int] = {}
        for pid, dep in self._pid_waiting.items():
            if self._pid_fn.get(pid) == fn_name:
                result[dep] = result.get(dep, 0) + 1
        return result

    def update_job_counts(self, fn_name: str, event: Event) -> None:
        """Fold one event into the per-fn job-count accumulator."""
        acc = self._acc.get(fn_name, self._agg.empty())
        self._acc[fn_name] = self._agg.combine(acc, self._agg.insert(event))

    def update(self, event: Event) -> None:
        """Ingest one bookman event, updating job stats and dependency wait state."""
        tag = event.dim("tag")
        pid_str = event.dim("pid")
        fn_name = event.dim("fn")

        if pid_str:
            pid = int(pid_str)
            if fn_name:
                record_pid_mapping(self._pid_fn, pid, fn_name, event)
            record_waiting_state(self._pid_waiting, pid, tag, event.dim("dep"))

        if fn_name:
            self.update_job_counts(fn_name, event)
