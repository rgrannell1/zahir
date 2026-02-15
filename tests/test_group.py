"""Tests for DependencyGroup"""

from datetime import UTC, datetime
from unittest.mock import Mock

from freezegun import freeze_time

from zahir.base_types import DependencyState
from zahir.dependencies.group import DependencyGroup
from zahir.dependencies.time import TimeDependency


@freeze_time("2025-01-01 12:00:00", tz_offset=0)
def test_dependency_group_all_satisfied():
    """Test that group is satisfied when all dependencies are satisfied."""
    # Both dependencies are satisfied at current time (12:00)
    dep1 = TimeDependency(before=None, after=datetime(2025, 1, 1, 10, 0, 0, tzinfo=UTC))
    dep2 = TimeDependency(before=None, after=datetime(2025, 1, 1, 11, 0, 0, tzinfo=UTC))

    group = DependencyGroup({"dep1": dep1, "dep2": dep2})
    assert group.satisfied().state == DependencyState.SATISFIED


@freeze_time("2025-01-01 12:00:00", tz_offset=0)
def test_dependency_group_one_unsatisfied():
    """Test that group is unsatisfied when any dependency is unsatisfied."""
    # First is satisfied, second is not (13:00 is in the future)
    dep1 = TimeDependency(before=None, after=datetime(2025, 1, 1, 10, 0, 0, tzinfo=UTC))
    dep2 = TimeDependency(before=None, after=datetime(2025, 1, 1, 13, 0, 0, tzinfo=UTC))

    group = DependencyGroup({"dep1": dep1, "dep2": dep2})
    assert group.satisfied().state == DependencyState.UNSATISFIED


@freeze_time("2025-01-01 12:00:00", tz_offset=0)
def test_dependency_group_one_impossible():
    """Test that group is impossible when any dependency is impossible."""
    dep1 = TimeDependency(before=None, after=datetime(2025, 1, 1, 10, 0, 0, tzinfo=UTC))
    # Second has already passed its 'before' deadline
    dep2 = TimeDependency(before=datetime(2025, 1, 1, 11, 0, 0, tzinfo=UTC), after=None)

    group = DependencyGroup({"dep1": dep1, "dep2": dep2})
    assert group.satisfied().state == DependencyState.IMPOSSIBLE


@freeze_time("2025-01-01 12:00:00", tz_offset=0)
def test_dependency_group_with_list():
    """Test that group handles lists of dependencies correctly."""
    dep1 = TimeDependency(before=None, after=datetime(2025, 1, 1, 10, 0, 0, tzinfo=UTC))
    dep2 = TimeDependency(before=None, after=datetime(2025, 1, 1, 11, 0, 0, tzinfo=UTC))
    dep3 = TimeDependency(before=None, after=datetime(2025, 1, 1, 11, 30, 0, tzinfo=UTC))

    # Mix single and list dependencies
    group = DependencyGroup({"single": dep1, "multi": [dep2, dep3]})
    assert group.satisfied().state == DependencyState.SATISFIED


@freeze_time("2025-01-01 12:00:00", tz_offset=0)
def test_dependency_group_with_list_one_unsatisfied():
    """Test that group is unsatisfied when any dependency in a list is unsatisfied."""
    dep1 = TimeDependency(before=None, after=datetime(2025, 1, 1, 10, 0, 0, tzinfo=UTC))
    dep2 = TimeDependency(before=None, after=datetime(2025, 1, 1, 11, 0, 0, tzinfo=UTC))
    # This one is not satisfied yet
    dep3 = TimeDependency(before=None, after=datetime(2025, 1, 1, 13, 0, 0, tzinfo=UTC))

    group = DependencyGroup({"single": dep1, "multi": [dep2, dep3]})
    assert group.satisfied().state == DependencyState.UNSATISFIED


def test_dependency_group_empty():
    """Test that empty group is satisfied."""
    group = DependencyGroup({})
    assert group.satisfied().state == DependencyState.SATISFIED


def test_dependency_group_save():
    """Test that save serializes all dependencies correctly."""
    from zahir.context.memory import MemoryContext
    from zahir.job_registry import SQLiteJobRegistry
    from zahir.scope import LocalScope

    dep1 = TimeDependency(before=None, after=datetime(2025, 1, 1, 10, 0, 0, tzinfo=UTC))
    dep2 = TimeDependency(before=None, after=datetime(2025, 1, 1, 11, 0, 0, tzinfo=UTC))
    dep3 = TimeDependency(before=datetime(2025, 1, 1, 14, 0, 0, tzinfo=UTC), after=None)

    group = DependencyGroup({"dep1": dep1, "dep2": [dep2, dep3]})

    scope = LocalScope()
    job_registry = SQLiteJobRegistry(":memory:")
    context = MemoryContext(scope=scope, job_registry=job_registry)
    saved = group.save(context)

    assert saved["type"] == "DependencyGroup"
    assert "dependencies" in saved
    assert "dep1" in saved["dependencies"]
    assert "dep2" in saved["dependencies"]

    # dep1 is a single dependency, saved as a list
    assert len(saved["dependencies"]["dep1"]) == 1
    assert saved["dependencies"]["dep1"][0]["type"] == "TimeDependency"

    # dep2 is a list of dependencies
    assert len(saved["dependencies"]["dep2"]) == 2
    assert saved["dependencies"]["dep2"][0]["type"] == "TimeDependency"
    assert saved["dependencies"]["dep2"][1]["type"] == "TimeDependency"


def test_dependency_group_save_load_roundtrip():
    """Test that save/load preserves the dependency group correctly."""
    from zahir.context.memory import MemoryContext
    from zahir.job_registry import SQLiteJobRegistry
    from zahir.scope import LocalScope

    dep1 = TimeDependency(before=None, after=datetime(2025, 1, 1, 10, 0, 0, tzinfo=UTC))
    dep2 = TimeDependency(before=None, after=datetime(2025, 1, 1, 11, 0, 0, tzinfo=UTC))

    group = DependencyGroup({"dep1": dep1, "dep2": dep2})

    scope = LocalScope()
    job_registry = SQLiteJobRegistry(":memory:")
    context = MemoryContext(scope=scope, job_registry=job_registry)
    saved = group.save(context)

    # Create mock context with scope

    context = Mock()
    # Return the class, not the result of .load
    context.scope.get_dependency_class.side_effect = lambda _type_name: TimeDependency

    loaded = DependencyGroup.load(context, saved)

    assert "dep1" in loaded.dependencies
    assert "dep2" in loaded.dependencies
    assert isinstance(loaded.dependencies["dep1"], list)
    assert isinstance(loaded.dependencies["dep2"], list)
