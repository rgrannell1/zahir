from bookman.events import Event
from zahir.progress_bar.progress_bar_state import (
    _ENQUEUE_TAG,
    _JOB_COMPLETE_TAG,
    _JOB_FAIL_TAG,
)


class TimeEstimator:
    """Estimates remaining workflow time from observed effect durations.

    Tracks mean duration per fn_name from completed spans, then multiplies
    by the number of in-flight jobs of each type to produce an ETA.
    """

    def __init__(self):
        self._durations: dict[str, list[float]] = {}
        self._in_flight: dict[str, int] = {}

    def _record_start(self, fn_name: str) -> None:
        self._in_flight[fn_name] = self._in_flight.get(fn_name, 0) + 1

    def _record_end(self, fn_name: str, duration_ms: float) -> None:
        # clamp to zero — workers run concurrently so events can arrive out of order
        self._in_flight[fn_name] = max(0, self._in_flight.get(fn_name, 0) - 1)
        self._durations.setdefault(fn_name, []).append(duration_ms)

    def update(self, event: Event) -> None:
        fn_name = event.dim("fn")
        if not fn_name:
            return

        tag = event.dim("tag")

        if tag in (_JOB_COMPLETE_TAG, _JOB_FAIL_TAG) and event.kind == "span":
            self._record_end(fn_name, event.duration("ms"))
        elif tag == _ENQUEUE_TAG and event.kind == "point":
            self._record_start(fn_name)

    def mean_duration_ms(self, fn_name: str) -> float | None:
        durations = self._durations.get(fn_name)
        if not durations:
            return None
        return sum(durations) / len(durations)

    def _estimated_remaining_ms(self) -> float | None:
        """Return estimated ms remaining, or None if no in-flight jobs or any type has no duration data."""
        total = 0.0
        has_in_flight = False

        for fn_name, count in self._in_flight.items():
            if count <= 0:
                continue
            has_in_flight = True
            mean = self.mean_duration_ms(fn_name)
            if mean is None:
                return None
            total += count * mean

        return total if has_in_flight else None

    def format_eta(self) -> str:
        ms = self._estimated_remaining_ms()
        if ms is None:
            return "--:--:--"
        total_seconds = int(ms / 1000)
        hours, remainder = divmod(total_seconds, 3600)
        minutes, seconds = divmod(remainder, 60)
        return f"{hours:02d}:{minutes:02d}:{seconds:02d}"
