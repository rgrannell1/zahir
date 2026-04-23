from dataclasses import dataclass

from bookman.events import Event

from zahir.core.constants import JobTag, Phase


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
        """Ingest one bookman event, updating job stats."""

        fn_name = event.dim("fn")

        if not fn_name:
            return

        tag = event.dim("tag")
        stats = self._stats(fn_name)

        if tag == JobTag.JOB_COMPLETE and event.dim("phase") == Phase.END:
            stats.completed += 1
        elif tag == JobTag.JOB_FAIL and event.dim("phase") == Phase.END:
            stats.failed += 1
        elif tag == JobTag.ENQUEUE and event.dim("phase") == Phase.START:
            stats.total += 1
            stats.started += 1
