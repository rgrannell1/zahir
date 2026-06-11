"""Tests for progress display row visibility rules."""

from zahir.progress_bar.progress_bar_state_model import JobStats
from zahir.progress_bar.rich_display_service import _visible_total


def test_visible_total_uses_started_when_enqueue_total_is_missing():
    """Proves executing jobs remain visible even if enqueue telemetry is absent."""

    assert _visible_total(JobStats(total=0, started=3)) == 3


def test_visible_total_prefers_enqueue_total_when_available():
    """Proves normal enqueued totals remain the display denominator."""

    assert _visible_total(JobStats(total=815, started=22)) == 815
