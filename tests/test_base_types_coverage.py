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
    DependencyResult,
    DependencyState,
    JobInformation,
    JobState,
)
from zahir.context.memory import MemoryContext
from zahir.dependencies.group import DependencyGroup
from zahir.events import JobOutputEvent, ZahirEvent
from zahir.job_registry import SQLiteJobRegistry
from zahir.scope import LocalScope


class MockDependency(Dependency):
    """Mock dependency for testing abstract methods."""

    def satisfied(self) -> DependencyResult:
        return DependencyResult(state=DependencyState.SATISFIED)

    def request_extension(self, extra_seconds: float) -> "MockDependency":
        return self

    def save(self, context: MemoryContext) -> Mapping[str, Any]:
        return {"type": "MockDependency"}

    @classmethod
    def load(cls, context: MemoryContext, data: Mapping[str, Any]) -> "MockDependency":
        return cls()


# Note: Abstract classes can't be instantiated in Python, so we test
# them indirectly through their concrete implementations


def test_dependency_state_enum():
    """Test DependencyState enum values."""
    assert DependencyState.SATISFIED == "satisfied"
    assert DependencyState.UNSATISFIED == "unsatisfied"
    assert DependencyState.IMPOSSIBLE == "impossible"


def test_job_information_dataclass():
    """Test JobInformation creation with all fields."""
    from datetime import datetime
    from zahir.base_types import JobInstance, JobArguments
    from zahir.jobs.decorator import spec

    @spec()
    def TestJobSpec(context: Context, input, dependencies):
        yield JobOutputEvent({"result": "test"})

    job_args = JobArguments(
        job_id="job-123",
        parent_id=None,
        args={},
        dependencies={},
        job_timeout=None,
        recover_timeout=None,
    )
    job = JobInstance(spec=TestJobSpec, args=job_args)

    job_info = JobInformation(
        job_id="job-123",
        job=job,
        state=JobState.COMPLETED,
        output={"result": "success"},
        started_at=datetime(2024, 1, 1, 12, 0, 0),
        completed_at=datetime(2024, 1, 1, 12, 5, 0),
    )

    assert job_info.job_id == "job-123"
    assert job_info.job == job
    assert job_info.state == JobState.COMPLETED
    assert job_info.output == {"result": "success"}


def test_context_dataclass():
    """Test Context dataclass creation."""
    scope = LocalScope()
    job_registry = SQLiteJobRegistry(":memory:")

    context = MemoryContext(scope=scope, job_registry=job_registry)

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


def test_completed_job_states_constant():
    """Test COMPLETED_JOB_STATES constant."""
    from zahir.base_types import COMPLETED_JOB_STATES

    assert JobState.IMPOSSIBLE in COMPLETED_JOB_STATES
    assert JobState.PRECHECK_FAILED in COMPLETED_JOB_STATES
    assert JobState.COMPLETED in COMPLETED_JOB_STATES
    assert JobState.RECOVERED in COMPLETED_JOB_STATES
    assert JobState.TIMED_OUT in COMPLETED_JOB_STATES
    assert JobState.RECOVERY_TIMED_OUT in COMPLETED_JOB_STATES
    assert JobState.IRRECOVERABLE in COMPLETED_JOB_STATES
    # Verify some non-completed states are not in the set
    assert JobState.PENDING not in COMPLETED_JOB_STATES
    assert JobState.RUNNING not in COMPLETED_JOB_STATES


def test_active_job_states_constant():
    """Test ACTIVE_JOB_STATES constant."""
    from zahir.base_types import ACTIVE_JOB_STATES

    assert JobState.PENDING in ACTIVE_JOB_STATES
    assert JobState.BLOCKED in ACTIVE_JOB_STATES
    assert JobState.READY in ACTIVE_JOB_STATES
    assert JobState.PAUSED in ACTIVE_JOB_STATES
    assert JobState.RECOVERING in ACTIVE_JOB_STATES
    # Verify terminal states are not in the set
    assert JobState.COMPLETED not in ACTIVE_JOB_STATES
    assert JobState.IRRECOVERABLE not in ACTIVE_JOB_STATES


def test_dependency_abstract_methods():
    """Test that abstract Dependency methods raise NotImplementedError when called directly."""
    import pytest

    from zahir.base_types import Dependency

    # Test the base class method directly by calling it on a mock instance
    # We use MockDependency but call the base class method
    dep = MockDependency()

    # Call the abstract method from the base class directly
    with pytest.raises(NotImplementedError):
        Dependency.satisfied(dep)


def test_dependency_abstract_save_method():
    """Test that Dependency.save raises NotImplementedError when not overridden."""
    import pytest

    from zahir.base_types import Dependency

    scope = LocalScope()
    job_registry = SQLiteJobRegistry(":memory:")
    context = MemoryContext(scope=scope, job_registry=job_registry)

    # Call the abstract method from the base class directly
    dep = MockDependency()

    with pytest.raises(NotImplementedError):
        Dependency.save(dep, context)


def test_dependency_abstract_load_method():
    """Test that Dependency.load raises NotImplementedError when not overridden."""
    import pytest

    from zahir.base_types import Dependency

    scope = LocalScope()
    job_registry = SQLiteJobRegistry(":memory:")
    context = MemoryContext(scope=scope, job_registry=job_registry)

    # Call the abstract method from the base class directly
    with pytest.raises(NotImplementedError):
        Dependency.load(context, {})
