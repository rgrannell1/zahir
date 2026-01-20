"""Tests for @spec decorator with recovery parameter."""

import tempfile

from zahir.base_types import Context
from zahir.context import MemoryContext
from zahir.dependencies.group import DependencyGroup
from zahir.events import JobOutputEvent, WorkflowOutputEvent
from zahir.job_registry import SQLiteJobRegistry
from zahir.jobs.decorator import spec
from zahir.scope import LocalScope


def test_job_decorator_without_recovery():
    """Test that @spec decorator works without recovery parameter."""

    @spec()
    def SimpleJob(context: Context, input, dependencies):
        """A simple job."""
        yield JobOutputEvent({"result": "success"})

    # Should create a Job class
    assert SimpleJob.__name__ == "SimpleJob"
    # Should use the default recover method from Job base class
    assert hasattr(SimpleJob, "recover")


def test_job_decorator_with_recovery():
    """Test that @spec decorator works with recovery parameter."""

    def recovery_fn(context: Context, input, dependencies, err):
        """Recovery function."""
        yield JobOutputEvent({"recovered": True, "error": str(err)})

    @spec(recover=recovery_fn)
    def JobWithRecovery(context: Context, input, dependencies):
        """A job with recovery."""
        raise ValueError("Test error")
        yield JobOutputEvent({"result": "should not reach"})

    # Should create a Job class with custom recovery
    assert JobWithRecovery.__name__ == "JobWithRecovery"
    assert hasattr(JobWithRecovery, "recover")


def test_job_decorator_parentheses_without_recovery():
    """Test that @spec() with empty parentheses works."""

    @spec()
    def JobWithParens(context: Context, input, dependencies):
        """A job defined with @spec()."""
        yield JobOutputEvent({"result": "works"})

    assert JobWithParens.__name__ == "JobWithParens"


def test_recovery_method_is_callable():
    """Test that the recovery method created by decorator is callable."""

    def my_recovery(context: Context, input, dependencies, err):
        """Recovery that returns success."""
        yield JobOutputEvent({"recovered": True})

    @spec(recover=my_recovery)
    def FailingJob(context: Context, input, dependencies):
        """A job that always fails."""
        raise RuntimeError("Intentional failure")
        yield JobOutputEvent({"result": "should not see this"})

    # Should be able to call the recover classmethod
    assert callable(FailingJob.recover)

    # Create a minimal context for testing
    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name
    job_registry = SQLiteJobRegistry(tmp_file)
    job_registry.init("test-worker")
    context = MemoryContext(scope=LocalScope(), job_registry=job_registry)

    # Test that it returns a generator
    err = RuntimeError("test")
    result = FailingJob.recover(context, {}, DependencyGroup({}), err)
    assert hasattr(result, "__iter__")

    # Test that it yields the expected output
    outputs = list(result)
    assert len(outputs) == 1
    assert isinstance(outputs[0], JobOutputEvent)
    assert outputs[0].output["recovered"] is True


def test_recovery_function_signature():
    """Test that recovery function receives all expected parameters."""

    captured_args = {}

    def capture_args_recovery(context: Context, input, dependencies, err):
        """Recovery that captures arguments."""
        
        captured_args["context"] = context
        captured_args["input"] = input
        captured_args["dependencies"] = dependencies
        captured_args["err"] = err
        yield JobOutputEvent({"done": True})

    @spec(recover=capture_args_recovery)
    def TestJob(context: Context, input, dependencies):
        """Test job."""
        yield JobOutputEvent({"result": "ok"})

    # Create a minimal context for testing
    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name
    job_registry = SQLiteJobRegistry(tmp_file)
    job_registry.init("test-worker")
    test_context = MemoryContext(scope=LocalScope(), job_registry=job_registry)
    test_input = {"key": "value"}
    test_dependencies = DependencyGroup({"dep": "value"})
    test_error = ValueError("test error")

    list(TestJob.recover(test_context, test_input, test_dependencies, test_error))

    # Verify all arguments were passed correctly
    
    assert captured_args["context"] == test_context
    assert captured_args["input"] == test_input
    assert captured_args["dependencies"] == test_dependencies
    assert captured_args["err"] == test_error


def test_job_without_recovery_uses_default():
    """Test that job without recovery parameter uses the default Job.recover behavior."""

    @spec()
    def NoRecoveryJob(context: Context, input, dependencies):
        """Job without custom recovery."""
        yield JobOutputEvent({"result": "ok"})

    # Should have recover method (inherited from Job)
    assert hasattr(NoRecoveryJob, "recover")

    # Create a minimal context for testing
    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name
    job_registry = SQLiteJobRegistry(tmp_file)
    job_registry.init("test-worker")
    context = MemoryContext(scope=LocalScope(), job_registry=job_registry)

    # Default recover should re-raise the error
    test_error = RuntimeError("test")
    try:
        # The default recover raises the error
        list(NoRecoveryJob.recover(context, {}, DependencyGroup({}), test_error))
        assert False, "Should have raised the error"
    except RuntimeError as err:
        assert str(err) == "test"


def test_recovery_can_yield_multiple_events():
    """Test that recovery function can yield multiple events."""

    def multi_event_recovery(context: Context, input, dependencies, err):
        """Recovery that yields multiple events."""
        yield WorkflowOutputEvent({"step": 1})
        yield WorkflowOutputEvent({"step": 2})
        yield JobOutputEvent({"final": True})

    @spec(recover=multi_event_recovery)
    def MultiEventJob(context: Context, input, dependencies):
        """Job with multi-event recovery."""
        yield JobOutputEvent({"result": "ok"})

    # Create a minimal context for testing
    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name
    job_registry = SQLiteJobRegistry(tmp_file)
    job_registry.init("test-worker")
    context = MemoryContext(scope=LocalScope(), job_registry=job_registry)

    # Test recovery yields all events
    events = list(MultiEventJob.recover(context, {}, DependencyGroup({}), RuntimeError("test")))
    assert len(events) == 3
    assert isinstance(events[0], WorkflowOutputEvent)
    assert isinstance(events[1], WorkflowOutputEvent)
    assert isinstance(events[2], JobOutputEvent)
