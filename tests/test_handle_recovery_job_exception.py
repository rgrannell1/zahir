"""Tests for the handle_recovery_job_exception state machine step.

Tests the handle_recovery_job_exception function which handles when a recovery
job raises an exception, marking the job as irrecoverable.
"""

import multiprocessing
import sys
import tempfile
import sys

from zahir.base_types import Context, JobState
from zahir.context import MemoryContext
from zahir.events import JobOutputEvent
from zahir.job_registry import SQLiteJobRegistry
from zahir.jobs.decorator import spec
from zahir.scope import LocalScope
import sys
from zahir.worker.call_frame import ZahirStackFrame
from zahir.worker.state_machine import ZahirWorkerState
from zahir.worker.state_machine.handle_recovery_job_exception import handle_recovery_job_exception
from zahir.worker.state_machine.states import WaitForJobStateChange


@spec()
def SimpleJob(context: Context, input, dependencies):
    """A simple job for testing."""
    yield JobOutputEvent({"result": "done"})


@spec()
def AnotherJob(context: Context, input, dependencies):
    """Another job for testing."""
    yield JobOutputEvent({"count": 1})


def test_handle_recovery_job_exception_returns_enqueue_state_change():
    """Test that handle_recovery_job_exception returns EnqueueJobStateChange."""

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    job_registry = SQLiteJobRegistry(tmp_file)
    job_registry.init("test-worker-1")

    context = MemoryContext(scope=LocalScope.from_module(sys.modules[__name__]), job_registry=job_registry)
    input_queue = multiprocessing.Queue()
    output_queue = multiprocessing.Queue()
    workflow_id = "test-workflow-1"

    worker_state = ZahirWorkerState(context, input_queue, output_queue, workflow_id)

    # Add a job
    job = SimpleJob({"test": "data"}, {}, 0.1)
    job_id = context.job_registry.add(context, job, output_queue)

    # Create a recovery frame
    job_generator = SimpleJob.recover(context, job.input, job.dependencies, None)
    worker_state.frame = ZahirStackFrame(job=job, job_generator=job_generator, recovery=True)

    # Set last_event to an exception
    worker_state.last_event = ValueError("Recovery failed")

    # Run handle_recovery_job_exception
    result, _ = handle_recovery_job_exception(worker_state)

    # Verify result
    assert isinstance(result, WaitForJobStateChange)
    assert "irrecoverably failed" in result.data["message"]
    assert "SimpleJob" in result.data["message"]


def test_handle_recovery_job_exception_sets_irrecoverable_state():
    """Test that the job state is set to IRRECOVERABLE."""

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    job_registry = SQLiteJobRegistry(tmp_file)
    job_registry.init("test-worker-2")

    context = MemoryContext(scope=LocalScope.from_module(sys.modules[__name__]), job_registry=job_registry)
    input_queue = multiprocessing.Queue()
    output_queue = multiprocessing.Queue()
    workflow_id = "test-workflow-2"

    worker_state = ZahirWorkerState(context, input_queue, output_queue, workflow_id)

    # Add a job
    job = SimpleJob({"test": "data"}, {}, 0.1)
    job_id = context.job_registry.add(context, job, output_queue)

    # Create a recovery frame
    job_generator = SimpleJob.recover(context, job.input, job.dependencies, None)
    worker_state.frame = ZahirStackFrame(job=job, job_generator=job_generator, recovery=True)

    # Set last_event to an exception
    worker_state.last_event = RuntimeError("Recovery exception")

    # Run handle_recovery_job_exception
    handle_recovery_job_exception(worker_state)

    # Verify job state
    job_state = context.job_registry.get_state(job.job_id)
    assert job_state == JobState.IRRECOVERABLE


def test_handle_recovery_job_exception_records_error():
    """Test that the exception from last_event is recorded."""

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    job_registry = SQLiteJobRegistry(tmp_file)
    job_registry.init("test-worker-3")

    context = MemoryContext(scope=LocalScope.from_module(sys.modules[__name__]), job_registry=job_registry)
    input_queue = multiprocessing.Queue()
    output_queue = multiprocessing.Queue()
    workflow_id = "test-workflow-3"

    worker_state = ZahirWorkerState(context, input_queue, output_queue, workflow_id)

    # Add a job
    job = SimpleJob({"test": "data"}, {}, 0.1)
    job_id = context.job_registry.add(context, job, output_queue)

    # Create a recovery frame
    job_generator = SimpleJob.recover(context, job.input, job.dependencies, None)
    worker_state.frame = ZahirStackFrame(job=job, job_generator=job_generator, recovery=True)

    # Set last_event to a specific exception
    test_exception = KeyError("specific recovery error")
    worker_state.last_event = test_exception

    # Run handle_recovery_job_exception
    handle_recovery_job_exception(worker_state)

    # Verify error was recorded
    errors = context.job_registry.get_errors(job.job_id, recovery=True)
    assert len(errors) > 0
    assert isinstance(errors[-1], KeyError)
    assert "specific recovery error" in str(errors[-1])


def test_handle_recovery_job_exception_clears_frame():
    """Test that the active frame is cleared."""

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    job_registry = SQLiteJobRegistry(tmp_file)
    job_registry.init("test-worker-4")

    context = MemoryContext(scope=LocalScope.from_module(sys.modules[__name__]), job_registry=job_registry)
    input_queue = multiprocessing.Queue()
    output_queue = multiprocessing.Queue()
    workflow_id = "test-workflow-4"

    worker_state = ZahirWorkerState(context, input_queue, output_queue, workflow_id)

    # Add a job
    job = SimpleJob({"test": "data"}, {}, 0.1)
    job_id = context.job_registry.add(context, job, output_queue)

    # Create a recovery frame
    job_generator = SimpleJob.recover(context, job.input, job.dependencies, None)
    worker_state.frame = ZahirStackFrame(job=job, job_generator=job_generator, recovery=True)
    worker_state.last_event = Exception("test")

    # Verify frame is set
    assert worker_state.frame is not None

    # Run handle_recovery_job_exception
    handle_recovery_job_exception(worker_state)

    # Verify frame is cleared
    assert worker_state.frame is None


def test_handle_recovery_job_exception_preserves_state():
    """Test that handle_recovery_job_exception returns the same state object."""

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    job_registry = SQLiteJobRegistry(tmp_file)
    job_registry.init("test-worker-5")

    context = MemoryContext(scope=LocalScope.from_module(sys.modules[__name__]), job_registry=job_registry)
    input_queue = multiprocessing.Queue()
    output_queue = multiprocessing.Queue()
    workflow_id = "test-workflow-5"

    worker_state = ZahirWorkerState(context, input_queue, output_queue, workflow_id)

    # Add a job
    job = SimpleJob({"test": "data"}, {}, 0.1)
    job_id = context.job_registry.add(context, job, output_queue)

    # Create a recovery frame
    job_generator = SimpleJob.recover(context, job.input, job.dependencies, None)
    worker_state.frame = ZahirStackFrame(job=job, job_generator=job_generator, recovery=True)
    worker_state.last_event = Exception("test")

    # Run handle_recovery_job_exception
    result, returned_state = handle_recovery_job_exception(worker_state)

    # Verify same state object returned
    assert returned_state is worker_state


def test_handle_recovery_job_exception_includes_job_type_in_message():
    """Test that the job type appears in the state change message."""

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    job_registry = SQLiteJobRegistry(tmp_file)
    job_registry.init("test-worker-6")

    context = MemoryContext(scope=LocalScope.from_module(sys.modules[__name__]), job_registry=job_registry)
    input_queue = multiprocessing.Queue()
    output_queue = multiprocessing.Queue()
    workflow_id = "test-workflow-6"

    worker_state = ZahirWorkerState(context, input_queue, output_queue, workflow_id)

    # Add a job
    job = AnotherJob({"count": 0}, {}, 0.1)
    job_id = context.job_registry.add(context, job, output_queue)

    # Create a recovery frame
    job_generator = AnotherJob.recover(context, job.input, job.dependencies, None)
    worker_state.frame = ZahirStackFrame(job=job, job_generator=job_generator, recovery=True)
    worker_state.last_event = Exception("test")

    # Run handle_recovery_job_exception
    result, _ = handle_recovery_job_exception(worker_state)

    # Verify job type in message
    assert "AnotherJob" in result.data["message"]


def test_handle_recovery_job_exception_transitions_to_enqueue():
    """Test that the state transitions to enqueue to start over."""

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    job_registry = SQLiteJobRegistry(tmp_file)
    job_registry.init("test-worker-7")

    context = MemoryContext(scope=LocalScope.from_module(sys.modules[__name__]), job_registry=job_registry)
    input_queue = multiprocessing.Queue()
    output_queue = multiprocessing.Queue()
    workflow_id = "test-workflow-7"

    worker_state = ZahirWorkerState(context, input_queue, output_queue, workflow_id)

    # Add a job
    job = SimpleJob({"test": "data"}, {}, 0.1)
    job_id = context.job_registry.add(context, job, output_queue)

    # Create a recovery frame
    job_generator = SimpleJob.recover(context, job.input, job.dependencies, None)
    worker_state.frame = ZahirStackFrame(job=job, job_generator=job_generator, recovery=True)
    worker_state.last_event = Exception("test")

    # Run handle_recovery_job_exception
    result, _ = handle_recovery_job_exception(worker_state)

    # Verify transition to wait for job
    assert isinstance(result, WaitForJobStateChange)
    # Frame should be cleared to allow enqueueing next job
    assert worker_state.frame is None


def test_handle_recovery_job_exception_with_different_exception_types():
    """Test that different exception types are handled correctly."""

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    job_registry = SQLiteJobRegistry(tmp_file)
    job_registry.init("test-worker-8")

    context = MemoryContext(scope=LocalScope.from_module(sys.modules[__name__]), job_registry=job_registry)
    input_queue = multiprocessing.Queue()
    output_queue = multiprocessing.Queue()
    workflow_id = "test-workflow-8"

    # Test with different exception types
    exception_types = [
        ValueError("value error"),
        TypeError("type error"),
        RuntimeError("runtime error"),
        KeyError("key error"),
    ]

    for idx, exc in enumerate(exception_types):
        worker_state = ZahirWorkerState(context, input_queue, output_queue, workflow_id)

        # Add a job
        job = SimpleJob({"test": f"data-{idx}"}, {})
        job_id = context.job_registry.add(context, job, output_queue)

        # Create a recovery frame
        job_generator = SimpleJob.recover(context, job.input, job.dependencies, None)
        worker_state.frame = ZahirStackFrame(job=job, job_generator=job_generator, recovery=True)
        worker_state.last_event = exc

        # Run handle_recovery_job_exception
        result, _ = handle_recovery_job_exception(worker_state)

        # Verify job is irrecoverable
        job_state = context.job_registry.get_state(job.job_id)
        assert job_state == JobState.IRRECOVERABLE

        # Verify error recorded
        errors = context.job_registry.get_errors(job.job_id, recovery=True)
        assert len(errors) > 0
        assert type(errors[-1]) == type(exc)
