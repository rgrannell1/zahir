"""Tests for dependency extension functionality"""

from datetime import UTC, datetime

from freezegun import freeze_time

from zahir.base_types import DependencyState
from zahir.context import MemoryContext
from zahir.dependencies.concurrency import ConcurrencyLimit
from zahir.dependencies.group import DependencyGroup
from zahir.dependencies.job import JobDependency
from zahir.dependencies.time import ExtensionMode, TimeDependency
from zahir.job_registry import SQLiteJobRegistry
from zahir.scope import LocalScope


@freeze_time("2025-01-01 12:00:00", tz_offset=0)
def test_time_dependency_extension_before_only():
    """Test extending only the 'before' time of a TimeDependency."""
    before_time = datetime(2025, 1, 1, 14, 0, 0, tzinfo=UTC)
    after_time = datetime(2025, 1, 1, 10, 0, 0, tzinfo=UTC)

    dep = TimeDependency(before=before_time, after=after_time, allow_extensions=ExtensionMode.BEFORE)

    # Request 1 hour (3600 seconds) extension
    extended = dep.request_extension(3600)

    # 'before' should be extended
    assert extended.before == datetime(2025, 1, 1, 15, 0, 0, tzinfo=UTC)
    # 'after' should remain unchanged
    assert extended.after == after_time
    assert extended.allow_extensions == ExtensionMode.BEFORE


@freeze_time("2025-01-01 12:00:00", tz_offset=0)
def test_time_dependency_extension_after_only():
    """Test extending only the 'after' time of a TimeDependency."""
    before_time = datetime(2025, 1, 1, 14, 0, 0, tzinfo=UTC)
    after_time = datetime(2025, 1, 1, 10, 0, 0, tzinfo=UTC)

    dep = TimeDependency(before=before_time, after=after_time, allow_extensions=ExtensionMode.AFTER)

    # Request 1 hour (3600 seconds) extension
    extended = dep.request_extension(3600)

    # 'before' should remain unchanged
    assert extended.before == before_time
    # 'after' should be extended
    assert extended.after == datetime(2025, 1, 1, 11, 0, 0, tzinfo=UTC)
    assert extended.allow_extensions == ExtensionMode.AFTER


@freeze_time("2025-01-01 12:00:00", tz_offset=0)
def test_time_dependency_extension_both():
    """Test extending both 'before' and 'after' times of a TimeDependency."""
    before_time = datetime(2025, 1, 1, 14, 0, 0, tzinfo=UTC)
    after_time = datetime(2025, 1, 1, 10, 0, 0, tzinfo=UTC)

    dep = TimeDependency(before=before_time, after=after_time, allow_extensions=ExtensionMode.BOTH)

    # Request 2 hours (7200 seconds) extension
    extended = dep.request_extension(7200)

    # Both should be extended by 2 hours
    assert extended.before == datetime(2025, 1, 1, 16, 0, 0, tzinfo=UTC)
    assert extended.after == datetime(2025, 1, 1, 12, 0, 0, tzinfo=UTC)
    assert extended.allow_extensions == ExtensionMode.BOTH


@freeze_time("2025-01-01 12:00:00", tz_offset=0)
def test_time_dependency_extension_none():
    """Test that extensions are not applied when mode is NONE."""
    before_time = datetime(2025, 1, 1, 14, 0, 0, tzinfo=UTC)
    after_time = datetime(2025, 1, 1, 10, 0, 0, tzinfo=UTC)

    dep = TimeDependency(before=before_time, after=after_time, allow_extensions=ExtensionMode.NONE)

    # Request 1 hour extension
    extended = dep.request_extension(3600)

    # Neither should be extended
    assert extended.before == before_time
    assert extended.after == after_time
    assert extended.allow_extensions == ExtensionMode.NONE


@freeze_time("2025-01-01 12:00:00", tz_offset=0)
def test_time_dependency_extension_with_none_values():
    """Test extending when before or after are None."""
    # Only 'before' is set
    dep1 = TimeDependency(
        before=datetime(2025, 1, 1, 14, 0, 0, tzinfo=UTC), after=None, allow_extensions=ExtensionMode.BOTH
    )

    extended1 = dep1.request_extension(3600)
    assert extended1.before == datetime(2025, 1, 1, 15, 0, 0, tzinfo=UTC)
    assert extended1.after is None

    # Only 'after' is set
    dep2 = TimeDependency(
        before=None, after=datetime(2025, 1, 1, 10, 0, 0, tzinfo=UTC), allow_extensions=ExtensionMode.BOTH
    )

    extended2 = dep2.request_extension(3600)
    assert extended2.before is None
    assert extended2.after == datetime(2025, 1, 1, 11, 0, 0, tzinfo=UTC)


@freeze_time("2025-01-01 12:00:00", tz_offset=0)
def test_time_dependency_extension_prevents_impossible():
    """Test that extending 'before' can prevent a dependency from becoming impossible."""
    # A dependency that's about to become impossible
    before_time = datetime(2025, 1, 1, 12, 30, 0, tzinfo=UTC)
    after_time = datetime(2025, 1, 1, 11, 0, 0, tzinfo=UTC)

    dep = TimeDependency(before=before_time, after=after_time, allow_extensions=ExtensionMode.BEFORE)

    # Currently satisfied (after 11:00, before 12:30)
    assert dep.satisfied().state == DependencyState.SATISFIED

    # Fast forward 45 minutes
    with freeze_time("2025-01-01 12:45:00", tz_offset=0):
        # Without extension, would be impossible
        assert dep.satisfied().state == DependencyState.IMPOSSIBLE

        # But with extension applied earlier, still satisfied
        extended = dep.request_extension(3600)  # Extend by 1 hour
        assert extended.before == datetime(2025, 1, 1, 13, 30, 0, tzinfo=UTC)
        assert extended.satisfied().state == DependencyState.SATISFIED


def test_time_dependency_extension_multiple_times():
    """Test that we can extend a dependency multiple times."""
    before_time = datetime(2025, 1, 1, 14, 0, 0, tzinfo=UTC)

    dep = TimeDependency(before=before_time, allow_extensions=ExtensionMode.BEFORE)

    # First extension: +30 minutes
    extended1 = dep.request_extension(1800)
    assert extended1.before == datetime(2025, 1, 1, 14, 30, 0, tzinfo=UTC)

    # Second extension: +another 30 minutes
    extended2 = extended1.request_extension(1800)
    assert extended2.before == datetime(2025, 1, 1, 15, 0, 0, tzinfo=UTC)

    # Third extension: +1 hour
    extended3 = extended2.request_extension(3600)
    assert extended3.before == datetime(2025, 1, 1, 16, 0, 0, tzinfo=UTC)


def test_concurrency_limit_extension_returns_self():
    """Test that ConcurrencyLimit returns itself unchanged for extensions."""
    scope = LocalScope()
    job_registry = SQLiteJobRegistry(":memory:")
    context = MemoryContext(scope=scope, job_registry=job_registry)

    limit = ConcurrencyLimit(limit=5, slots=2, context=context)

    extended = limit.request_extension(3600)

    # Should return itself unchanged
    assert extended is limit
    assert extended.limit == 5
    assert extended.slots == 2


def test_job_dependency_extension_returns_self():
    """Test that JobDependency returns itself unchanged for extensions."""
    registry = SQLiteJobRegistry(":memory:")
    dep = JobDependency("test-job-id", registry)

    extended = dep.request_extension(3600)

    # Should return itself unchanged
    assert extended is dep
    assert extended.job_id == "test-job-id"


def test_dependency_group_extension_propagates():
    """Test that DependencyGroup propagates extensions to all child dependencies."""
    before_time = datetime(2025, 1, 1, 14, 0, 0, tzinfo=UTC)
    after_time = datetime(2025, 1, 1, 10, 0, 0, tzinfo=UTC)

    scope = LocalScope()
    job_registry = SQLiteJobRegistry(":memory:")
    context = MemoryContext(scope=scope, job_registry=job_registry)

    group = DependencyGroup({
        "time1": TimeDependency(before=before_time, allow_extensions=ExtensionMode.BEFORE),
        "time2": TimeDependency(after=after_time, allow_extensions=ExtensionMode.AFTER),
        "job": JobDependency("test-job", job_registry),
        "concurrency": ConcurrencyLimit(limit=5, slots=1, context=context),
    })

    # Request 1 hour extension
    extended = group.request_extension(3600)

    # TimeDependency with 'before' should be extended
    time1 = extended.dependencies["time1"]
    assert time1.before == datetime(2025, 1, 1, 15, 0, 0, tzinfo=UTC)

    # TimeDependency with 'after' should be extended
    time2 = extended.dependencies["time2"]
    assert time2.after == datetime(2025, 1, 1, 11, 0, 0, tzinfo=UTC)

    # Other dependencies should be unchanged but present
    assert "job" in extended.dependencies
    assert "concurrency" in extended.dependencies


def test_dependency_group_extension_with_list():
    """Test that DependencyGroup propagates extensions to lists of dependencies."""
    before_time1 = datetime(2025, 1, 1, 14, 0, 0, tzinfo=UTC)
    before_time2 = datetime(2025, 1, 1, 15, 0, 0, tzinfo=UTC)

    group = DependencyGroup({
        "times": [
            TimeDependency(before=before_time1, allow_extensions=ExtensionMode.BEFORE),
            TimeDependency(before=before_time2, allow_extensions=ExtensionMode.BEFORE),
        ]
    })

    # Request 30 minute extension
    extended = group.request_extension(1800)

    # Both dependencies in the list should be extended
    times = extended.dependencies["times"]
    assert isinstance(times, list)
    assert len(times) == 2
    assert times[0].before == datetime(2025, 1, 1, 14, 30, 0, tzinfo=UTC)
    assert times[1].before == datetime(2025, 1, 1, 15, 30, 0, tzinfo=UTC)


def test_dependency_group_extension_nested():
    """Test that DependencyGroup propagates extensions to nested groups."""
    before_time = datetime(2025, 1, 1, 14, 0, 0, tzinfo=UTC)

    scope = LocalScope()
    job_registry = SQLiteJobRegistry(":memory:")
    context = MemoryContext(scope=scope, job_registry=job_registry)

    inner_group = DependencyGroup({"time": TimeDependency(before=before_time, allow_extensions=ExtensionMode.BEFORE)})

    outer_group = DependencyGroup({"inner": inner_group, "limit": ConcurrencyLimit(limit=3, slots=1, context=context)})

    # Request 1 hour extension
    extended = outer_group.request_extension(3600)

    # Extension should propagate through nested groups
    inner = extended.dependencies["inner"]
    time_dep = inner.dependencies["time"]
    assert time_dep.before == datetime(2025, 1, 1, 15, 0, 0, tzinfo=UTC)


@freeze_time("2025-01-01 12:00:00", tz_offset=0)
def test_extension_use_case_retry_with_backoff():
    """Test a realistic use case: extending time window for job retries with backoff."""
    # A job should run between 10:00 and 14:00
    after_time = datetime(2025, 1, 1, 10, 0, 0, tzinfo=UTC)
    before_time = datetime(2025, 1, 1, 14, 0, 0, tzinfo=UTC)

    dep = TimeDependency(before=before_time, after=after_time, allow_extensions=ExtensionMode.BEFORE)

    # Currently at 12:00, dependency is satisfied
    assert dep.satisfied().state == DependencyState.SATISFIED

    # Simulate retry after 2.5 hours (job took longer than expected)
    with freeze_time("2025-01-01 14:30:00", tz_offset=0):
        # Without extension, dependency would be impossible
        assert dep.satisfied().state == DependencyState.IMPOSSIBLE

        # But we can request an extension for retry (e.g., 2 hours backoff)
        extended = dep.request_extension(7200)

        # Now the dependency should be satisfied with the new window
        assert extended.before == datetime(2025, 1, 1, 16, 0, 0, tzinfo=UTC)
        assert extended.satisfied().state == DependencyState.SATISFIED
