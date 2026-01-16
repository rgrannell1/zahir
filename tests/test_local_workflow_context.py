"""Tests for LocalWorkflow default context creation from calling scope."""

from zahir.base_types import Context, Dependency, DependencyState
from zahir.events import JobOutputEvent
from zahir.jobs.decorator import spec
from zahir.worker import LocalWorkflow
import sys


# Define jobs and dependencies at module level that should be discovered
@spec()
def WorkflowTestJob1(spec_args, context: Context, input, dependencies):
    """A test job that should be discovered by LocalWorkflow."""
    yield JobOutputEvent({"result": "job1_output"})


@spec()
def WorkflowTestJob2(spec_args, context: Context, input, dependencies):
    """Another test job that should be discovered by LocalWorkflow."""
    yield JobOutputEvent({"result": "job2_output"})


class WorkflowTestDependency1(Dependency):
    """A test dependency that should be discovered by LocalWorkflow."""

    def satisfied(self) -> DependencyState:
        return DependencyState.SATISFIED

    def request_extension(self, extra_seconds: float):
        return self

    def save(self, context):
        return {"type": "WorkflowTestDependency1"}

    @classmethod
    def load(cls, context, data):
        return cls()


class WorkflowTestDependency2(Dependency):
    """Another test dependency that should be discovered by LocalWorkflow."""

    def satisfied(self) -> DependencyState:
        return DependencyState.UNSATISFIED

    def request_extension(self, extra_seconds: float):
        return self

    def save(self, context):
        return {"type": "WorkflowTestDependency2"}

    @classmethod
    def load(cls, context, data):
        return cls()


def test_localworkflow_creates_default_context():
    """Test that LocalWorkflow creates a context when none is provided."""

    workflow = LocalWorkflow()

    assert workflow.context is not None
    assert workflow.context.scope is not None
    assert workflow.context.job_registry is not None


def test_localworkflow_discovers_jobs_from_calling_module():
    """Test that LocalWorkflow discovers jobs from the calling module's scope."""

    workflow = LocalWorkflow()

    # Verify the scope discovered our test jobs
    assert workflow.context is not None
    assert workflow.context.scope is not None

    # Check that specs were discovered
    assert "WorkflowTestJob1" in workflow.context.scope.specs
    assert "WorkflowTestJob2" in workflow.context.scope.specs

    # Check that they're the correct specs
    assert workflow.context.scope.specs["WorkflowTestJob1"] == WorkflowTestJob1
    assert workflow.context.scope.specs["WorkflowTestJob2"] == WorkflowTestJob2


def test_localworkflow_discovers_dependencies_from_calling_module():
    """Test that LocalWorkflow discovers dependencies from the calling module's scope."""

    workflow = LocalWorkflow()

    # Verify the scope discovered our test dependencies
    assert workflow.context is not None
    assert workflow.context.scope is not None

    # Check that dependencies were discovered
    assert "WorkflowTestDependency1" in workflow.context.scope.dependencies
    assert "WorkflowTestDependency2" in workflow.context.scope.dependencies

    # Check that they're the correct classes
    assert workflow.context.scope.dependencies["WorkflowTestDependency1"] == WorkflowTestDependency1
    assert workflow.context.scope.dependencies["WorkflowTestDependency2"] == WorkflowTestDependency2


def test_localworkflow_can_run_discovered_jobs():
    """Test that LocalWorkflow can actually execute jobs discovered from the calling module."""

    workflow = LocalWorkflow()

    # Create an instance of a discovered job
    job_instance = WorkflowTestJob1({}, {})

    # Run the workflow with events_filter=None to see all internal events
    events = list(workflow.run(job_instance, events_filter=None))

    # The workflow should execute and complete successfully
    # We should get at least some events back from the workflow execution
    assert len(events) > 0

    # Verify we get basic workflow events
    from zahir.events import JobEvent, WorkflowCompleteEvent, WorkflowStartedEvent

    started_events = [event for event in events if isinstance(event, WorkflowStartedEvent)]
    assert len(started_events) == 1

    job_events = [event for event in events if isinstance(event, JobEvent)]
    assert len(job_events) == 1
    assert job_events[0].job["type"] == "WorkflowTestJob1"

    complete_events = [event for event in events if isinstance(event, WorkflowCompleteEvent)]
    assert len(complete_events) == 1


def test_localworkflow_respects_provided_context():
    """Test that LocalWorkflow uses a provided context instead of creating one."""

    from zahir.context import MemoryContext
    from zahir.job_registry.sqlite import SQLiteJobRegistry
    from zahir.scope import LocalScope

    # Create a custom context with a custom job
    @spec()
    def CustomJob(spec_args, context: Context, input, dependencies):
        yield JobOutputEvent({"custom": "result"})

    custom_scope = LocalScope.from_module(sys.modules[__name__])
    custom_context = MemoryContext(scope=custom_scope, job_registry=SQLiteJobRegistry(":memory:"))

    workflow = LocalWorkflow(custom_context)

    # Should use the provided context, not create a new one
    assert workflow.context is custom_context
    # The custom context has module-level specs (WorkflowTestJob1, etc.)
    assert "WorkflowTestJob1" in workflow.context.scope.specs
    # But it should not have other module's specs
    assert "WorkflowTestJob3" not in workflow.context.scope.specs


def test_localworkflow_default_context_uses_memory_registry():
    """Test that the default context uses an in-memory SQLite database."""

    workflow = LocalWorkflow()

    # The context should have a job registry
    assert workflow.context is not None
    assert workflow.context.job_registry is not None

    # Verify it's a SQLiteJobRegistry with in-memory database
    from zahir.job_registry.sqlite import SQLiteJobRegistry

    assert isinstance(workflow.context.job_registry, SQLiteJobRegistry)
    assert workflow.context.job_registry._db_path == ":memory:"
