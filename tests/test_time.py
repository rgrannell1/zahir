"""Tests for TimeDependency"""

from datetime import UTC, datetime
from unittest.mock import Mock

from freezegun import freeze_time

from zahir.base_types import DependencyState
from zahir.context.memory import MemoryContext
from zahir.dependencies.time import TimeDependency


@freeze_time("2025-01-01 13:00:00", tz_offset=0)
def test_time_dependency_satisfied_after_time():
    """Test that dependency is satisfied when current time is after the 'after' time."""
    after_time = datetime(2025, 1, 1, 12, 0, 0, tzinfo=UTC)
    dep = TimeDependency(before=None, after=after_time)
    assert dep.satisfied().state == DependencyState.SATISFIED


@freeze_time("2025-01-01 11:00:00", tz_offset=0)
def test_time_dependency_unsatisfied_before_time():
    """Test that dependency is unsatisfied when current time is before the 'after' time."""
    after_time = datetime(2025, 1, 1, 12, 0, 0, tzinfo=UTC)
    dep = TimeDependency(before=None, after=after_time)
    assert dep.satisfied().state == DependencyState.UNSATISFIED


@freeze_time("2025-01-01 13:00:00", tz_offset=0)
def test_time_dependency_impossible_after_before_time():
    """Test that dependency is impossible when current time is after the 'before' time."""
    before_time = datetime(2025, 1, 1, 12, 0, 0, tzinfo=UTC)
    dep = TimeDependency(before=before_time, after=None)
    assert dep.satisfied().state == DependencyState.IMPOSSIBLE


@freeze_time("2025-01-01 12:00:00", tz_offset=0)
def test_time_dependency_window_satisfied():
    """Test time window dependency when current time is within valid window."""
    after_time = datetime(2025, 1, 1, 10, 0, 0, tzinfo=UTC)
    before_time = datetime(2025, 1, 1, 14, 0, 0, tzinfo=UTC)
    dep = TimeDependency(before=before_time, after=after_time)
    assert dep.satisfied().state == DependencyState.SATISFIED


@freeze_time("2025-01-01 09:00:00", tz_offset=0)
def test_time_dependency_window_too_early():
    """Test time window dependency when current time is before the window."""
    after_time = datetime(2025, 1, 1, 10, 0, 0, tzinfo=UTC)
    before_time = datetime(2025, 1, 1, 14, 0, 0, tzinfo=UTC)
    dep = TimeDependency(before=before_time, after=after_time)
    assert dep.satisfied().state == DependencyState.UNSATISFIED


@freeze_time("2025-01-01 15:00:00", tz_offset=0)
def test_time_dependency_window_too_late():
    """Test time window dependency when current time is after the window."""
    after_time = datetime(2025, 1, 1, 10, 0, 0, tzinfo=UTC)
    before_time = datetime(2025, 1, 1, 14, 0, 0, tzinfo=UTC)
    dep = TimeDependency(before=before_time, after=after_time)
    assert dep.satisfied().state == DependencyState.IMPOSSIBLE


def test_time_dependency_no_constraints():
    """Test that dependency with no time constraints is always satisfied."""
    dep = TimeDependency(before=None, after=None)
    assert dep.satisfied().state == DependencyState.SATISFIED


def test_time_dependency_save_load_roundtrip():
    """Test that save/load preserves the dependency correctly."""
    from zahir.context.memory import MemoryContext
    from zahir.job_registry import SQLiteJobRegistry
    from zahir.scope import LocalScope

    before_time = datetime(2025, 1, 1, 14, 0, 0, tzinfo=UTC)
    after_time = datetime(2025, 1, 1, 10, 0, 0, tzinfo=UTC)

    dep = TimeDependency(before=before_time, after=after_time)

    scope = LocalScope()
    job_registry = SQLiteJobRegistry(":memory:")
    context = MemoryContext(scope=scope, job_registry=job_registry)

    # Save
    saved = dep.save(context)

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
    from zahir.context.memory import MemoryContext
    from zahir.job_registry import SQLiteJobRegistry
    from zahir.scope import LocalScope

    dep = TimeDependency(before=None, after=None)

    scope = LocalScope()
    job_registry = SQLiteJobRegistry(":memory:")
    context = MemoryContext(scope=scope, job_registry=job_registry)

    saved = dep.save(context)
    assert saved["before"] is None
    assert saved["after"] is None

    context = Mock()
    loaded = TimeDependency.load(context, saved)
    assert loaded.before is None
    assert loaded.after is None


def test_time_dependency_save_load_partial():
    """Test save/load with only one time constraint."""
    from zahir.context.memory import MemoryContext
    from zahir.job_registry import SQLiteJobRegistry
    from zahir.scope import LocalScope

    after_time = datetime(2025, 1, 1, 10, 0, 0, tzinfo=UTC)

    dep = TimeDependency(before=None, after=after_time)

    scope = LocalScope()
    job_registry = SQLiteJobRegistry(":memory:")
    context = MemoryContext(scope=scope, job_registry=job_registry)

    saved = dep.save(context)
    context = Mock()
    loaded = TimeDependency.load(context, saved)

    assert loaded.before is None
    assert loaded.after == after_time
