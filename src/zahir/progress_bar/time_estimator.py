# Estimates remaining workflow time from completed job durations and in-flight counts
from collections import defaultdict

from bookman.events import Event

from zahir.emit import PHASE_END, PHASE_START
from zahir.progress_bar.progress_bar_state import ENQUEUE_TAG, JOB_COMPLETE_TAG, JOB_FAIL_TAG

_JOB_END_TAGS = {JOB_COMPLETE_TAG, JOB_FAIL_TAG}


def _format_ms(ms: float) -> str:
    total_seconds = int(ms / 1000)
    hours, remainder = divmod(total_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}"


class TimeEstimator:
    """Estimates remaining workflow time from observed effect durations.

    Tracks mean duration per fn_name from completed spans, then multiplies
    by the number of in-flight jobs of each type to produce an ETA.
    """

    def __init__(self):
        self._durations: defaultdict[str, list[float]] = defaultdict(list)
        self._in_flight: defaultdict[str, int] = defaultdict(int)

    def _record_start(self, fn_name: str) -> None:
        """Mark one more job of this type as in-flight."""

        self._in_flight[fn_name] += 1

    def _record_end(self, fn_name: str, duration_ms: float) -> None:
        """Decrement in-flight count and record the completed duration."""

        # clamp to zero — workers run concurrently so events can arrive out of order
        if self._in_flight[fn_name] > 0:
            self._in_flight[fn_name] -= 1
        self._durations[fn_name].append(duration_ms)

    def update(self, event: Event) -> None:
        """Ingest one bookman event, updating in-flight counts or duration history."""

        fn_name = event.dim("fn")
        if not fn_name:
            return
        tag = event.dim("tag")
        phase = event.dim("phase")

        if tag in _JOB_END_TAGS and phase == PHASE_END:
            self._record_end(fn_name, event.duration("ms"))
        elif tag == ENQUEUE_TAG and phase == PHASE_START:
            self._record_start(fn_name)

    def mean_duration_ms(self, fn_name: str) -> float | None:
        """Return the mean completed duration for this job type, or None if no data."""

        durations = self._durations.get(fn_name)

        if not durations:
            return None

        return sum(durations) / len(durations)

    def _estimated_remaining_ms(self) -> float | None:
        """Sum (in-flight count × mean duration) across all job types."""

        in_flight = [(fn, count) for fn, count in self._in_flight.items() if count > 0]

        if not in_flight:
            return None

        total = 0.0
        # estimate mean for each function type, multiply by in-flight count, sum
        for fn, count in in_flight:
            mean = self.mean_duration_ms(fn)
            if mean is None:
                return None
            total += count * mean

        return total

    def format_eta(self) -> str:
        """Return a human-readable ETA string"""

        ms = self._estimated_remaining_ms()
        return "--:--:--" if ms is None else _format_ms(ms)
