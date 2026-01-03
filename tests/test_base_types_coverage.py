"""Tests to improve code coverage for base_types.py

Focusing on uncovered lines:
- Line 21-23: TYPE_CHECKING branch
- Line 54, 66, 74, 79: Abstract method raise NotImplementedError
- Line 180, 196, 202, 208, 214, 220: More abstract methods
"""

from collections.abc import Iterator, Mapping
from typing import Any

from zahir.base_types import (
    Context,
    Dependency,
    DependencyData,
    DependencyState,
    EventRegistry,
    Job,
    JobInformation,
    JobOptions,
    JobState,
)
from zahir.dependencies.group import DependencyGroup
from zahir.events import JobOutputEvent, ZahirEvent
from zahir.job_registry import SQLiteJobRegistry
from zahir.scope import LocalScope


class MockDependency(Dependency):
    """Mock dependency for testing abstract methods."""

    def satisfied(self) -> DependencyState:
        return DependencyState.SATISFIED

    def request_extension(self, extra_seconds: float) -> "MockDependency":
        return self

    def save(self) -> Mapping[str, Any]:
        return {"type": "MockDependency"}

    @classmethod
    def load(cls, context: Context, data: Mapping[str, Any]) -> "MockDependency":
        return cls()


class MockEventRegistry(EventRegistry):
    """Mock event registry for testing."""

    def __init__(self):
        self.events: list[ZahirEvent] = []

    def register(self, event: ZahirEvent) -> None:
        self.events.append(event)


# Note: Abstract classes can't be instantiated in Python, so we test
# them indirectly through their concrete implementations


def test_job_options_save_and_load():
    """Test JobOptions serialization and deserialization."""
    # Test with both timeouts set
    options = JobOptions(job_timeout=30.0, recover_timeout=60.0)
    saved = options.save()

    assert saved["job_timeout"] == 30.0
    assert saved["recover_timeout"] == 60.0

    loaded = JobOptions.load(saved)
    assert loaded.job_timeout == 30.0
    assert loaded.recover_timeout == 60.0

    # Test with None values
    options_none = JobOptions()
    saved_none = options_none.save()

    assert saved_none["job_timeout"] is None
    assert saved_none["recover_timeout"] is None

    loaded_none = JobOptions.load(saved_none)
    assert loaded_none.job_timeout is None
    assert loaded_none.recover_timeout is None


def test_job_options_load_partial():
    """Test JobOptions loading when some fields are missing."""
    partial_data = {"job_timeout": 25.0}
    loaded = JobOptions.load(partial_data)  # type: ignore

    assert loaded.job_timeout == 25.0
    assert loaded.recover_timeout is None


def test_dependency_state_enum():
    """Test DependencyState enum values."""
    assert DependencyState.SATISFIED == "satisfied"
    assert DependencyState.UNSATISFIED == "unsatisfied"
    assert DependencyState.IMPOSSIBLE == "impossible"


def test_job_information_dataclass():
    """Test JobInformation creation with all fields."""
    from datetime import datetime

    class TestJob(Job):
        @classmethod
        def run(cls, context: Context, input: dict, dependencies: DependencyGroup) -> Iterator[Job | JobOutputEvent]:
            yield JobOutputEvent({"result": "test"})

    job = TestJob(input={}, dependencies={})

    job_info = JobInformation(
        job_id="job-123",
        job=job,
        state=JobState.COMPLETED,
        output={"result": "success"},
        started_at=datetime(2024, 1, 1, 12, 0, 0),
        completed_at=datetime(2024, 1, 1, 12, 5, 0),
        duration_seconds=300.0,
        recovery_duration_seconds=10.0,
    )

    assert job_info.job_id == "job-123"
    assert job_info.job == job
    assert job_info.state == JobState.COMPLETED
    assert job_info.output == {"result": "success"}
    assert job_info.duration_seconds == 300.0
    assert job_info.recovery_duration_seconds == 10.0


def test_context_dataclass():
    """Test Context dataclass creation."""
    scope = LocalScope()
    job_registry = SQLiteJobRegistry(":memory:")

    context = Context(scope=scope, job_registry=job_registry)

    assert context.scope == scope
    assert context.job_registry == job_registry


def test_dependency_data_typed_dict():
    """Test DependencyData TypedDict structure."""
    # This tests that DependencyData can be created with type field
    dep_data: DependencyData = {"type": "test_dependency"}
    assert dep_data["type"] == "test_dependency"


def test_job_state_enum_all_values():
    """Test all JobState enum values."""
    assert JobState.PENDING == "pending"
    assert JobState.BLOCKED == "blocked"
    assert JobState.IMPOSSIBLE == "impossible"
    assert JobState.READY == "ready"
    assert JobState.PAUSED == "paused"
    assert JobState.PRECHECK_FAILED == "precheck_failed"
    assert JobState.RUNNING == "running"
    assert JobState.COMPLETED == "completed"
    assert JobState.RECOVERING == "recovering"
    assert JobState.RECOVERED == "recovered"
    assert JobState.TIMED_OUT == "timed_out"
    assert JobState.RECOVERY_TIMED_OUT == "recovery_timed_out"
    assert JobState.IRRECOVERABLE == "irrecoverable"
