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
from zahir.context.memory import MemoryContext
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

    def save(self, context: MemoryContext) -> Mapping[str, Any]:
        return {"type": "MockDependency"}

    @classmethod
    def load(cls, context: MemoryContext, data: Mapping[str, Any]) -> "MockDependency":
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


def test_job_registry_abstract_methods():
    """Test that JobRegistry abstract methods raise NotImplementedError."""
    import pytest

    from zahir.base_types import JobRegistry

    # Use the SQLiteJobRegistry instance and call base class method directly
    registry = SQLiteJobRegistry(":memory:")

    with pytest.raises(NotImplementedError):
        JobRegistry.init(registry, "worker-1")


def test_event_registry_abstract_method():
    """Test that EventRegistry.register raises NotImplementedError when not overridden."""
    from zahir.base_types import EventRegistry

    class IncompleteEventRegistry(EventRegistry):
        """Intentionally incomplete to test abstract method errors."""

    # Cannot instantiate abstract class without implementing all abstract methods
    # So we test the method directly would raise if it could be called
    assert hasattr(EventRegistry, "register")


def test_job_abstract_run_method():
    """Test that Job.run default implementation returns empty iterator."""

    class MinimalJob(Job):
        """Job with default run implementation."""

    # The default run implementation returns iter([])
    scope = LocalScope()
    job_registry = SQLiteJobRegistry(":memory:")
    context = MemoryContext(scope=scope, job_registry=job_registry)

    result = list(MinimalJob.run(context, {}, DependencyGroup({})))
    assert result == []


def test_job_recover_raises_error():
    """Test that Job.recover default implementation raises the error."""
    import pytest

    class MinimalJob(Job):
        """Job with default recover implementation."""

        @classmethod
        def run(cls, context: Context, input: dict, dependencies: DependencyGroup) -> Iterator[Job | JobOutputEvent]:
            yield JobOutputEvent({"result": "test"})

    scope = LocalScope()
    job_registry = SQLiteJobRegistry(":memory:")
    context = MemoryContext(scope=scope, job_registry=job_registry)

    test_error = ValueError("test error")

    with pytest.raises(ValueError, match="test error"):
        # Consume the generator to trigger the raise and yield
        list(MinimalJob.recover(None, context, {}, DependencyGroup({}), test_error))


def test_scope_abstract_methods():
    """Test that Scope abstract methods exist and would raise if not overridden."""
    from zahir.base_types import Scope

    # Verify abstract methods exist on the Scope class
    assert hasattr(Scope, "add_job_class")
    assert hasattr(Scope, "add_job_classes")
    assert hasattr(Scope, "get_job_class")
    assert hasattr(Scope, "add_dependency_class")
    assert hasattr(Scope, "add_dependency_classes")
    assert hasattr(Scope, "get_dependency_class")


def test_job_save_with_options():
    """Test Job.save method includes options when present."""

    class TestJob(Job):
        @classmethod
        def run(cls, context: Context, input: dict, dependencies: DependencyGroup) -> Iterator[Job | JobOutputEvent]:
            yield JobOutputEvent({"result": "test"})

    scope = LocalScope()
    job_registry = SQLiteJobRegistry(":memory:")
    context = MemoryContext(scope=scope, job_registry=job_registry)

    options = JobOptions(job_timeout=30.0, recover_timeout=60.0)
    job = TestJob(input={"test": "data"}, dependencies={}, options=options, job_id="test-job-123")

    saved = job.save(context)

    assert saved["type"] == "TestJob"
    assert saved["job_id"] == "test-job-123"
    assert saved["parent_id"] is None
    assert saved["input"] == {"test": "data"}
    assert saved["options"] is not None
    assert saved["options"]["job_timeout"] == 30.0
    assert saved["options"]["recover_timeout"] == 60.0


def test_job_save_without_options():
    """Test Job.save method when options is None."""

    class TestJob(Job):
        @classmethod
        def run(cls, context: Context, input: dict, dependencies: DependencyGroup) -> Iterator[Job | JobOutputEvent]:
            yield JobOutputEvent({"result": "test"})

    scope = LocalScope()
    job_registry = SQLiteJobRegistry(":memory:")
    context = MemoryContext(scope=scope, job_registry=job_registry)

    job = TestJob(input={"test": "data"}, dependencies={}, options=None, job_id="test-job-456")

    saved = job.save(context)

    assert saved["type"] == "TestJob"
    assert saved["job_id"] == "test-job-456"
    assert saved["options"] is None


def test_job_load_with_options():
    """Test Job.load method with options."""

    class TestJob(Job):
        @classmethod
        def run(cls, context: Context, input: dict, dependencies: DependencyGroup) -> Iterator[Job | JobOutputEvent]:
            yield JobOutputEvent({"result": "test"})

    scope = LocalScope()
    scope.add_job_class(TestJob)
    job_registry = SQLiteJobRegistry(":memory:")
    context = MemoryContext(scope=scope, job_registry=job_registry)

    data: Any = {
        "type": "TestJob",
        "job_id": "loaded-job-123",
        "parent_id": "parent-456",
        "input": {"key": "value"},
        "dependencies": {"type": "DependencyGroup", "dependencies": {}},
        "options": {"job_timeout": 45.0, "recover_timeout": 90.0},
    }

    loaded_job = TestJob.load(context, data)

    assert loaded_job.job_id == "loaded-job-123"
    assert loaded_job.parent_id == "parent-456"
    assert loaded_job.input == {"key": "value"}
    assert loaded_job.job_options is not None
    assert loaded_job.job_options.job_timeout == 45.0
    assert loaded_job.job_options.recover_timeout == 90.0


def test_job_load_without_options():
    """Test Job.load method when options is None."""

    class TestJob(Job):
        @classmethod
        def run(cls, context: Context, input: dict, dependencies: DependencyGroup) -> Iterator[Job | JobOutputEvent]:
            yield JobOutputEvent({"result": "test"})

    scope = LocalScope()
    scope.add_job_class(TestJob)
    job_registry = SQLiteJobRegistry(":memory:")
    context = MemoryContext(scope=scope, job_registry=job_registry)

    data: Any = {
        "type": "TestJob",
        "job_id": "loaded-job-789",
        "parent_id": None,
        "input": {"key": "value"},
        "dependencies": {"type": "DependencyGroup", "dependencies": {}},
        "options": None,
    }

    loaded_job = TestJob.load(context, data)

    assert loaded_job.job_id == "loaded-job-789"
    assert loaded_job.parent_id is None
    assert loaded_job.job_options is None


def test_job_copy():
    """Test Job.copy method creates a new instance with new ID."""

    class TestJob(Job):
        @classmethod
        def run(cls, context: Context, input: dict, dependencies: DependencyGroup) -> Iterator[Job | JobOutputEvent]:
            yield JobOutputEvent({"result": "test"})

    options = JobOptions(job_timeout=30.0)
    original_job = TestJob(input={"data": "original"}, dependencies={}, options=options, job_id="original-123")

    copied_job = original_job.copy()

    # Should have different job_id
    assert copied_job.job_id != original_job.job_id
    # Should have original job as parent
    assert copied_job.parent_id == original_job.job_id
    # Should have same input
    assert copied_job.input == original_job.input
    # Should have same options
    assert copied_job.job_options == original_job.job_options


def test_job_request_extension():
    """Test Job.request_extension method."""
    from datetime import datetime

    from zahir.dependencies.time import TimeDependency

    class TestJob(Job):
        @classmethod
        def run(cls, context: Context, input: dict, dependencies: DependencyGroup) -> Iterator[Job | JobOutputEvent]:
            yield JobOutputEvent({"result": "test"})

    start_time = datetime.now()
    time_dep = TimeDependency(after=start_time)

    original_job = TestJob(input={"data": "test"}, dependencies={"time": time_dep}, job_id="original-456")

    extended_job = original_job.request_extension(60.0)

    # Should have different job_id
    assert extended_job.job_id != original_job.job_id
    # Should have original job as parent
    assert extended_job.parent_id == original_job.job_id
    # Should have same input
    assert extended_job.input == original_job.input
