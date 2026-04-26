from dataclasses import dataclass

from bookman.events import Event

from zahir.core.constants import DependencyTag, JobTag, Phase


@dataclass
class JobStats:
    total: int = 0
    started: int = 0
    completed: int = 0
    failed: int = 0

    @property
    def processed(self) -> int:
        return self.completed + self.failed


def record_pid_mapping(pid_fn: dict[int, str], pid: int, fn_name: str, tag: str, phase: str) -> None:
    """Update the pid→fn_name map when a worker starts a new job."""
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
    elif tag in (DependencyTag.SATISFIED, JobTag.JOB_COMPLETE, JobTag.JOB_FAIL, JobTag.ENQUEUE):
        pid_waiting.pop(pid, None)


def record_job_stats(stats: JobStats, tag: str, phase: str) -> None:
    """Increment the appropriate counter on the job stats struct."""
    if tag == JobTag.JOB_COMPLETE and phase == Phase.END:
        stats.completed += 1
    elif tag == JobTag.JOB_FAIL and phase == Phase.END:
        stats.failed += 1
    elif tag == JobTag.ENQUEUE and phase == Phase.START:
        stats.total += 1
        stats.started += 1


class ProgressBarState:
    def __init__(self):
        self.jobs: dict[str, JobStats] = {}
        self._pid_fn: dict[int, str] = {}
        self._pid_waiting: dict[int, str] = {}

    def _stats(self, fn_name: str) -> JobStats:
        return self.jobs.setdefault(fn_name, JobStats())

    def waiting_deps(self, fn_name: str) -> dict[str, int]:
        """Return {dep_label: count} for jobs of this fn_name currently blocked on a dependency."""
        result: dict[str, int] = {}
        for pid, dep in self._pid_waiting.items():
            if self._pid_fn.get(pid) == fn_name:
                result[dep] = result.get(dep, 0) + 1
        return result

    def update(self, event: Event) -> None:
        """Ingest one bookman event, updating job stats and dependency wait state."""
        tag = event.dim("tag")
        phase = event.dim("phase")
        pid_str = event.dim("pid")
        fn_name = event.dim("fn")

        if pid_str:
            pid = int(pid_str)
            if fn_name:
                record_pid_mapping(self._pid_fn, pid, fn_name, tag, phase)
            record_waiting_state(self._pid_waiting, pid, tag, event.dim("dep"))

        if fn_name:
            record_job_stats(self._stats(fn_name), tag, phase)
