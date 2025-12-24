"""Tests for TimeDependency"""

from datetime import datetime, timezone
from unittest.mock import Mock
from freezegun import freeze_time
from zahir.dependencies.time import TimeDependency
from zahir.types import DependencyState


@freeze_time("2025-01-01 13:00:00", tz_offset=0)
def test_time_dependency_satisfied_after_time():
    """Test that dependency is satisfied when current time is after the 'after' time."""
    after_time = datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    dep = TimeDependency(before=None, after=after_time)
    assert dep.satisfied() == DependencyState.SATISFIED


@freeze_time("2025-01-01 11:00:00", tz_offset=0)
def test_time_dependency_unsatisfied_before_time():
    """Test that dependency is unsatisfied when current time is before the 'after' time."""
    after_time = datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    dep = TimeDependency(before=None, after=after_time)
    assert dep.satisfied() == DependencyState.UNSATISFIED


@freeze_time("2025-01-01 13:00:00", tz_offset=0)
def test_time_dependency_impossible_after_before_time():
    """Test that dependency is impossible when current time is after the 'before' time."""
    before_time = datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    dep = TimeDependency(before=before_time, after=None)
    assert dep.satisfied() == DependencyState.IMPOSSIBLE


@freeze_time("2025-01-01 12:00:00", tz_offset=0)
def test_time_dependency_window_satisfied():
    """Test time window dependency when current time is within valid window."""
    after_time = datetime(2025, 1, 1, 10, 0, 0, tzinfo=timezone.utc)
    before_time = datetime(2025, 1, 1, 14, 0, 0, tzinfo=timezone.utc)
    dep = TimeDependency(before=before_time, after=after_time)
    assert dep.satisfied() == DependencyState.SATISFIED


@freeze_time("2025-01-01 09:00:00", tz_offset=0)
def test_time_dependency_window_too_early():
    """Test time window dependency when current time is before the window."""
    after_time = datetime(2025, 1, 1, 10, 0, 0, tzinfo=timezone.utc)
    before_time = datetime(2025, 1, 1, 14, 0, 0, tzinfo=timezone.utc)
    dep = TimeDependency(before=before_time, after=after_time)
    assert dep.satisfied() == DependencyState.UNSATISFIED


@freeze_time("2025-01-01 15:00:00", tz_offset=0)
def test_time_dependency_window_too_late():
    """Test time window dependency when current time is after the window."""
    after_time = datetime(2025, 1, 1, 10, 0, 0, tzinfo=timezone.utc)
    before_time = datetime(2025, 1, 1, 14, 0, 0, tzinfo=timezone.utc)
    dep = TimeDependency(before=before_time, after=after_time)
    assert dep.satisfied() == DependencyState.IMPOSSIBLE


def test_time_dependency_no_constraints():
    """Test that dependency with no time constraints is always satisfied."""
    dep = TimeDependency(before=None, after=None)
    assert dep.satisfied() == DependencyState.SATISFIED


def test_time_dependency_save_load_roundtrip():
    """Test that save/load preserves the dependency correctly."""
    before_time = datetime(2025, 1, 1, 14, 0, 0, tzinfo=timezone.utc)
    after_time = datetime(2025, 1, 1, 10, 0, 0, tzinfo=timezone.utc)

    dep = TimeDependency(before=before_time, after=after_time)

    # Save
    saved = dep.save()

    # Verify saved structure
    assert saved["type"] == "TimeDependency"
    assert saved["before"] == before_time.isoformat()
    assert saved["after"] == after_time.isoformat()

    # Load with mock context
    context = Mock()
    loaded = TimeDependency.load(context, saved)

    # Verify loaded dependency matches original
    assert loaded.before == before_time
    assert loaded.after == after_time


def test_time_dependency_save_load_with_none():
    """Test save/load with None values."""
    dep = TimeDependency(before=None, after=None)

    saved = dep.save()
    assert saved["before"] is None
    assert saved["after"] is None

    context = Mock()
    loaded = TimeDependency.load(context, saved)
    assert loaded.before is None
    assert loaded.after is None


def test_time_dependency_save_load_partial():
    """Test save/load with only one time constraint."""
    after_time = datetime(2025, 1, 1, 10, 0, 0, tzinfo=timezone.utc)

    dep = TimeDependency(before=None, after=after_time)

    saved = dep.save()
    context = Mock()
    loaded = TimeDependency.load(context, saved)

    assert loaded.before is None
    assert loaded.after == after_time
