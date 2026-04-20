from dataclasses import dataclass, field

from zahir.progress_bar.events import ZahirSpanEnd, ZahirTelemetryEvent


@dataclass
class JobStats:
    total: int = 0  # jobs enqueued (incremented at start, before we know the outcome)
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

    def update(self, event: ZahirTelemetryEvent) -> None:
        """Update state from a ZahirTelemetryEvent or ZahirSpanEnd."""
        fn_name = event.attributes.get("fn_name")
        if not fn_name:
            return

        stats = self._stats(fn_name)

        match event:
            case ZahirSpanEnd(error=None):
                stats.completed += 1
            case ZahirSpanEnd(error=_):
                stats.failed += 1
            case ZahirTelemetryEvent(event="start"):
                stats.total += 1
                stats.started += 1
