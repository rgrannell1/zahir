from dataclasses import dataclass

from bookman.events import Event

from zahir.emit import PHASE_END, PHASE_START

_ENQUEUE_TAG = "enqueue"
_JOB_COMPLETE_TAG = "job_complete"
_JOB_FAIL_TAG = "job_fail"


@dataclass
class JobStats:
    total: int = 0
    started: int = 0
    completed: int = 0
    failed: int = 0

    @property
    def processed(self) -> int:
        return self.completed + self.failed


class ProgressBarState:
    def __init__(self):
        self.jobs: dict[str, JobStats] = {}

    def _stats(self, fn_name: str) -> JobStats:
        return self.jobs.setdefault(fn_name, JobStats())

    def update(self, event: Event) -> None:
        fn_name = event.dim("fn")
        if not fn_name:
            return

        tag = event.dim("tag")
        stats = self._stats(fn_name)

        if tag == _JOB_COMPLETE_TAG and event.dim("phase") == PHASE_END:
            stats.completed += 1
        elif tag == _JOB_FAIL_TAG and event.dim("phase") == PHASE_END:
            stats.failed += 1
        elif tag == _ENQUEUE_TAG and event.dim("phase") == PHASE_START:
            stats.total += 1
            stats.started += 1
