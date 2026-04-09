"""Tests for DEFAULT_MESSAGE and .message() override on dependencies."""

from datetime import UTC, datetime
from unittest.mock import Mock

from freezegun import freeze_time

from zahir.base_types import DependencyState, JobState
from zahir.context.memory import MemoryContext
from zahir.dependencies.concurrency import ConcurrencyLimit
from zahir.dependencies.group import DependencyGroup
from zahir.dependencies.job import JobDependency
from zahir.dependencies.resources import ResourceLimit
from zahir.dependencies.semaphore import Semaphore
from zahir.dependencies.sqlite import SqliteDependency
from zahir.dependencies.time import TimeDependency
from zahir.job_registry import SQLiteJobRegistry
from zahir.scope import LocalScope


def make_context() -> MemoryContext:
    return MemoryContext(scope=LocalScope(), job_registry=SQLiteJobRegistry(":memory:"))


# ---------------------------------------------------------------------------
# TimeDependency
# ---------------------------------------------------------------------------

def test_time_dependency_default_message_satisfied() -> None:
    dep = TimeDependency()
    result = dep.satisfied()
    assert result.state == DependencyState.SATISFIED
    assert result.message is not None
    assert "satisfied" in result.message.lower()


@freeze_time("2025-01-01 11:00:00", tz_offset=0)
def test_time_dependency_default_message_unsatisfied() -> None:
    dep = TimeDependency(after=datetime(2025, 1, 1, 12, 0, 0, tzinfo=UTC))
    result = dep.satisfied()
    assert result.state == DependencyState.UNSATISFIED
    assert result.message is not None


@freeze_time("2025-01-01 13:00:00", tz_offset=0)
def test_time_dependency_default_message_impossible() -> None:
    dep = TimeDependency(before=datetime(2025, 1, 1, 12, 0, 0, tzinfo=UTC))
    result = dep.satisfied()
    assert result.state == DependencyState.IMPOSSIBLE
    assert result.message is not None


def test_time_dependency_message_override() -> None:
    dep = TimeDependency().message({
        DependencyState.SATISFIED: "All good.",
        DependencyState.UNSATISFIED: "Not yet.",
        DependencyState.IMPOSSIBLE: "Never.",
    })
    result = dep.satisfied()
    assert result.message == "All good."


def test_time_dependency_message_override_with_metadata() -> None:
    dep = TimeDependency().message({
        DependencyState.SATISFIED: "after={after} before={before}",
    })
    result = dep.satisfied()
    assert result.message == "after=None before=None"


def test_time_dependency_override_preserved_across_extension() -> None:
    dep = TimeDependency().message({DependencyState.SATISFIED: "custom"})
    extended = dep.request_extension(60.0)
    result = extended.satisfied()
    assert result.message == "custom"


def test_time_dependency_save_load_roundtrip_with_override() -> None:
    context = make_context()
    dep = TimeDependency().message({DependencyState.SATISFIED: "round-trip"})
    saved = dep.save(context)
    assert "message_override" in saved

    loaded = TimeDependency.load(context, saved)
    result = loaded.satisfied()
    assert result.message == "round-trip"


# ---------------------------------------------------------------------------
# JobDependency
# ---------------------------------------------------------------------------

def test_job_dependency_default_message_satisfied() -> None:
    registry = Mock()
    registry.get_state.return_value = JobState.COMPLETED
    dep: JobDependency[dict] = JobDependency("job-abc", registry)
    result = dep.satisfied()
    assert result.message == "Job job-abc completed (completed)."


def test_job_dependency_default_message_impossible() -> None:
    registry = Mock()
    registry.get_state.return_value = JobState.IRRECOVERABLE
    dep: JobDependency[dict] = JobDependency("job-xyz", registry)
    result = dep.satisfied()
    assert result.state == DependencyState.IMPOSSIBLE
    assert result.message == "Job job-xyz could not complete (irrecoverable)."


def test_job_dependency_message_override() -> None:
    registry = Mock()
    registry.get_state.return_value = JobState.COMPLETED
    dep: JobDependency[dict] = JobDependency("job-1", registry).message({
        DependencyState.SATISFIED: "done",
    })
    result = dep.satisfied()
    assert result.message == "done"


def test_job_dependency_save_load_roundtrip_with_override() -> None:
    context = make_context()
    registry = Mock()
    registry.get_state.return_value = JobState.COMPLETED

    dep: JobDependency[dict] = JobDependency("job-2", registry).message({DependencyState.SATISFIED: "job done"})
    saved = dep.save(context)
    assert "message_override" in saved

    mock_context = Mock()
    mock_context.job_registry = registry
    loaded = JobDependency.load(mock_context, saved)
    result = loaded.satisfied()
    assert result.message == "job done"


# ---------------------------------------------------------------------------
# DependencyGroup
# ---------------------------------------------------------------------------

def test_dependency_group_default_message_satisfied() -> None:
    group = DependencyGroup({"time": TimeDependency()})
    result = group.satisfied()
    assert result.state == DependencyState.SATISFIED
    assert result.message == "All dependencies satisfied."


def test_dependency_group_default_message_unsatisfied() -> None:
    registry = Mock()
    registry.get_state.return_value = JobState.PENDING
    group = DependencyGroup({"job": JobDependency("j", registry)})
    result = group.satisfied()
    assert result.state == DependencyState.UNSATISFIED
    assert result.message is not None


def test_dependency_group_message_override() -> None:
    group = DependencyGroup({"time": TimeDependency()}).message({
        DependencyState.SATISFIED: "all clear",
    })
    result = group.satisfied()
    assert result.message == "all clear"


def test_dependency_group_override_preserved_across_extension() -> None:
    group = DependencyGroup({"time": TimeDependency()}).message({
        DependencyState.SATISFIED: "extended",
    })
    extended = group.request_extension(10.0)
    result = extended.satisfied()
    assert result.message == "extended"


# ---------------------------------------------------------------------------
# SqliteDependency
# ---------------------------------------------------------------------------

def test_sqlite_dependency_default_message_satisfied() -> None:
    dep = SqliteDependency(db_path=":memory:", query="SELECT 'satisfied' AS status")
    result = dep.satisfied()
    assert result.state == DependencyState.SATISFIED
    assert result.message is not None
    assert ":memory:" in result.message


def test_sqlite_dependency_message_override() -> None:
    dep = SqliteDependency(db_path=":memory:", query="SELECT 'satisfied' AS status").message({
        DependencyState.SATISFIED: "sqlite ok",
    })
    result = dep.satisfied()
    assert result.message == "sqlite ok"


def test_sqlite_dependency_save_load_roundtrip_with_override() -> None:
    context = make_context()
    dep = SqliteDependency(db_path=":memory:", query="SELECT 'satisfied' AS status").message({
        DependencyState.SATISFIED: "persisted",
    })
    saved = dep.save(context)
    assert "message_override" in saved

    loaded = SqliteDependency.load(context, saved)
    result = loaded.satisfied()
    assert result.message == "persisted"


# ---------------------------------------------------------------------------
# Semaphore
# ---------------------------------------------------------------------------

def test_semaphore_default_message_satisfied() -> None:
    context = make_context()
    dep = Semaphore(context=context)
    result = dep.satisfied()
    assert result.state == DependencyState.SATISFIED
    assert result.message is not None
    assert dep.semaphore_id in result.message


def test_semaphore_default_message_closed() -> None:
    context = make_context()
    dep = Semaphore(context=context, initial_state=DependencyState.UNSATISFIED)
    result = dep.satisfied()
    assert result.state == DependencyState.UNSATISFIED
    assert result.message is not None


def test_semaphore_message_override() -> None:
    context = make_context()
    dep = Semaphore(context=context).message({DependencyState.SATISFIED: "gate open"})
    result = dep.satisfied()
    assert result.message == "gate open"


def test_semaphore_save_load_roundtrip_with_override() -> None:
    context = make_context()
    dep = Semaphore(context=context).message({DependencyState.SATISFIED: "s-ok"})
    saved = dep.save(context)
    assert "message_override" in saved

    loaded = Semaphore.load(context, saved)
    result = loaded.satisfied()
    assert result.message == "s-ok"


# ---------------------------------------------------------------------------
# ResourceLimit
# ---------------------------------------------------------------------------

def test_resource_limit_default_message_satisfied() -> None:
    dep = ResourceLimit(resource="cpu", max_percent=100.0)
    result = dep.satisfied()
    assert result.state == DependencyState.SATISFIED
    assert result.message is not None
    assert "cpu" in result.message


def test_resource_limit_message_override() -> None:
    dep = ResourceLimit(resource="cpu", max_percent=100.0).message({
        DependencyState.SATISFIED: "cpu fine",
    })
    result = dep.satisfied()
    assert result.message == "cpu fine"


def test_resource_limit_save_load_roundtrip_with_override() -> None:
    context = make_context()
    dep = ResourceLimit(resource="memory", max_percent=100.0).message({
        DependencyState.SATISFIED: "mem fine",
    })
    saved = dep.save(context)
    assert "message_override" in saved

    loaded = ResourceLimit.load(context, saved)
    result = loaded.satisfied()
    assert result.message == "mem fine"


# ---------------------------------------------------------------------------
# ConcurrencyLimit
# ---------------------------------------------------------------------------

def test_concurrency_limit_default_message_satisfied() -> None:
    context = make_context()
    dep = ConcurrencyLimit(limit=4, slots=1, context=context)
    result = dep.satisfied()
    assert result.state == DependencyState.SATISFIED
    assert result.message is not None
    assert "1" in result.message


def test_concurrency_limit_message_override() -> None:
    context = make_context()
    dep = ConcurrencyLimit(limit=4, slots=1, context=context).message({
        DependencyState.SATISFIED: "slot acquired",
    })
    result = dep.satisfied()
    assert result.message == "slot acquired"


def test_concurrency_limit_save_load_roundtrip_with_override() -> None:
    context = make_context()
    dep = ConcurrencyLimit(limit=2, slots=1, context=context).message({
        DependencyState.SATISFIED: "concurrency ok",
    })
    saved = dep.save(context)
    assert "message_override" in saved

    loaded = ConcurrencyLimit.load(context, saved)
    result = loaded.satisfied()
    assert result.message == "concurrency ok"


# ---------------------------------------------------------------------------
# Override falls back to DEFAULT_MESSAGE for states not in the override dict
# ---------------------------------------------------------------------------

def test_override_falls_back_to_default_for_unlisted_state() -> None:
    dep = TimeDependency().message({DependencyState.UNSATISFIED: "custom unsatisfied"})
    result = dep.satisfied()
    assert result.state == DependencyState.SATISFIED
    assert result.message == TimeDependency.DEFAULT_MESSAGE[DependencyState.SATISFIED].format(
        before=None, after=None
    )


def test_no_message_when_state_absent_from_both_override_and_default() -> None:
    class NoMessageDep(TimeDependency):
        DEFAULT_MESSAGE: dict[DependencyState, str] = {}

    result = NoMessageDep().satisfied()
    assert result.message is None
