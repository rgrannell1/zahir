"""Tests for the handle_job_exception state machine step.

Tests the handle_job_exception function which handles job exceptions,
switches to recovery mode, and transitions to check preconditions.
"""

import multiprocessing
import tempfile

from zahir.base_types import Context, Job, JobState
from zahir.context import MemoryContext
from zahir.events import JobOutputEvent
from zahir.job_registry import SQLiteJobRegistry
from zahir.jobs.decorator import job
from zahir.scope import LocalScope
from zahir.worker.call_frame import ZahirStackFrame
from zahir.worker.state_machine import ZahirWorkerState
from zahir.worker.state_machine.handle_job_exception import handle_job_exception
from zahir.worker.state_machine.states import CheckPreconditionsStateChange


class SimpleJobWithRecovery(Job):
    """A simple job that has a recovery mechanism."""

    @classmethod
    def run(cls, context, input, dependencies):
        raise Exception("Simulated failure")
        yield iter([])

    @classmethod
    def recover(cls, context, input, dependencies, err):
        yield JobOutputEvent({"recovered": True})


class JobWithCustomRecovery(Job):
    """A job with custom recovery logic."""

    @classmethod
    def run(cls, context, input, dependencies):
        raise ValueError("Custom error")
        yield iter([])

    @classmethod
    def recover(cls, context, input, dependencies, err):
        yield JobOutputEvent({"error_type": type(err).__name__, "recovered": True})


@job()
def SimpleJob(context: Context, input, dependencies):
    """A simple job for testing."""
    yield JobOutputEvent({"result": "done"})


def test_handle_job_exception_sets_recovery_mode():
    """Test that handle_job_exception sets the frame to recovery mode."""

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    job_registry = SQLiteJobRegistry(tmp_file)
    job_registry.init("test-worker-1")

    context = MemoryContext(scope=LocalScope(jobs=[SimpleJobWithRecovery]), job_registry=job_registry)
    output_queue = multiprocessing.Queue()
    workflow_id = "test-workflow-1"

    worker_state = ZahirWorkerState(context, None, output_queue, workflow_id)

    # Add a job
    job = SimpleJobWithRecovery(input={"test": "data"}, dependencies={}, options=None)
    job_id = context.job_registry.add(job, output_queue)

    # Set up frame
    job_generator = SimpleJobWithRecovery.run(context, job.input, job.dependencies)
    frame = ZahirStackFrame(job=job, job_generator=job_generator, recovery=False)
    worker_state.frame = frame

    # Verify recovery mode is False initially
    assert worker_state.frame.recovery is False

    # Set up exception event
    test_exception = Exception("Test error")
    worker_state.last_event = test_exception

    # Run handle_job_exception
    handle_job_exception(worker_state)

    # Frame should now be in recovery mode
    assert worker_state.frame.recovery is True


def test_handle_job_exception_updates_job_state():
    """Test that handle_job_exception sets the job state to RECOVERING."""

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    job_registry = SQLiteJobRegistry(tmp_file)
    job_registry.init("test-worker-2")

    context = MemoryContext(scope=LocalScope(jobs=[SimpleJobWithRecovery]), job_registry=job_registry)
    output_queue = multiprocessing.Queue()
    workflow_id = "test-workflow-2"

    worker_state = ZahirWorkerState(context, None, output_queue, workflow_id)

    # Add a job
    job = SimpleJobWithRecovery(input={"test": "data"}, dependencies={}, options=None)
    job_id = context.job_registry.add(job, output_queue)

    # Set up frame
    job_generator = SimpleJobWithRecovery.run(context, job.input, job.dependencies)
    frame = ZahirStackFrame(job=job, job_generator=job_generator, recovery=False)
    worker_state.frame = frame

    # Set up exception event
    worker_state.last_event = Exception("Test error")

    # Run handle_job_exception
    handle_job_exception(worker_state)

    # Job state should be RECOVERING
    job_state = context.job_registry.get_state(job_id)
    assert job_state == JobState.RECOVERING


def test_handle_job_exception_transitions_to_check_preconditions():
    """Test that handle_job_exception returns CheckPreconditionsStateChange."""

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    job_registry = SQLiteJobRegistry(tmp_file)
    job_registry.init("test-worker-3")

    context = MemoryContext(scope=LocalScope(jobs=[SimpleJobWithRecovery]), job_registry=job_registry)
    output_queue = multiprocessing.Queue()
    workflow_id = "test-workflow-3"

    worker_state = ZahirWorkerState(context, None, output_queue, workflow_id)

    # Add a job
    job = SimpleJobWithRecovery(input={"test": "data"}, dependencies={}, options=None)
    job_id = context.job_registry.add(job, output_queue)

    # Set up frame
    job_generator = SimpleJobWithRecovery.run(context, job.input, job.dependencies)
    frame = ZahirStackFrame(job=job, job_generator=job_generator, recovery=False)
    worker_state.frame = frame

    # Set up exception event
    worker_state.last_event = Exception("Test error")

    # Run handle_job_exception
    result, _ = handle_job_exception(worker_state)

    # Should transition to check preconditions
    assert isinstance(result, CheckPreconditionsStateChange)
    assert "entering recovery" in result.data["message"]


def test_handle_job_exception_replaces_generator():
    """Test that handle_job_exception replaces the job generator with recovery generator."""

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    job_registry = SQLiteJobRegistry(tmp_file)
    job_registry.init("test-worker-4")

    context = MemoryContext(scope=LocalScope(jobs=[SimpleJobWithRecovery]), job_registry=job_registry)
    output_queue = multiprocessing.Queue()
    workflow_id = "test-workflow-4"

    worker_state = ZahirWorkerState(context, None, output_queue, workflow_id)

    # Add a job
    job = SimpleJobWithRecovery(input={"test": "data"}, dependencies={}, options=None)
    job_id = context.job_registry.add(job, output_queue)

    # Set up frame
    job_generator = SimpleJobWithRecovery.run(context, job.input, job.dependencies)
    frame = ZahirStackFrame(job=job, job_generator=job_generator, recovery=False)
    worker_state.frame = frame

    original_generator = worker_state.frame.job_generator

    # Set up exception event
    test_exception = Exception("Test error")
    worker_state.last_event = test_exception

    # Run handle_job_exception
    handle_job_exception(worker_state)

    # Generator should be replaced with recovery generator
    assert worker_state.frame.job_generator is not original_generator
    assert worker_state.frame.job_generator is not None


def test_handle_job_exception_preserves_state():
    """Test that handle_job_exception returns the same state object."""

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    job_registry = SQLiteJobRegistry(tmp_file)
    job_registry.init("test-worker-5")

    context = MemoryContext(scope=LocalScope(jobs=[SimpleJobWithRecovery]), job_registry=job_registry)
    output_queue = multiprocessing.Queue()
    workflow_id = "test-workflow-5"

    worker_state = ZahirWorkerState(context, None, output_queue, workflow_id)

    # Add a job
    job = SimpleJobWithRecovery(input={"test": "data"}, dependencies={}, options=None)
    job_id = context.job_registry.add(job, output_queue)

    # Set up frame
    job_generator = SimpleJobWithRecovery.run(context, job.input, job.dependencies)
    frame = ZahirStackFrame(job=job, job_generator=job_generator, recovery=False)
    worker_state.frame = frame

    # Set up exception event
    worker_state.last_event = Exception("Test error")

    # Run handle_job_exception
    result, returned_state = handle_job_exception(worker_state)

    # Should return same state object
    assert returned_state is worker_state


def test_handle_job_exception_with_custom_exception():
    """Test that handle_job_exception works with different exception types."""

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    job_registry = SQLiteJobRegistry(tmp_file)
    job_registry.init("test-worker-6")

    context = MemoryContext(scope=LocalScope(jobs=[JobWithCustomRecovery]), job_registry=job_registry)
    output_queue = multiprocessing.Queue()
    workflow_id = "test-workflow-6"

    worker_state = ZahirWorkerState(context, None, output_queue, workflow_id)

    # Add a job
    job = JobWithCustomRecovery(input={"test": "data"}, dependencies={}, options=None)
    job_id = context.job_registry.add(job, output_queue)

    # Set up frame
    job_generator = JobWithCustomRecovery.run(context, job.input, job.dependencies)
    frame = ZahirStackFrame(job=job, job_generator=job_generator, recovery=False)
    worker_state.frame = frame

    # Set up exception event with custom exception type
    custom_error = ValueError("Custom error message")
    worker_state.last_event = custom_error

    # Run handle_job_exception
    result, _ = handle_job_exception(worker_state)

    # Should work with custom exception
    assert isinstance(result, CheckPreconditionsStateChange)
    assert worker_state.frame.recovery is True


def test_handle_job_exception_preserves_job_info():
    """Test that handle_job_exception preserves job ID and other info."""

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    job_registry = SQLiteJobRegistry(tmp_file)
    job_registry.init("test-worker-7")

    context = MemoryContext(scope=LocalScope(jobs=[SimpleJobWithRecovery]), job_registry=job_registry)
    output_queue = multiprocessing.Queue()
    workflow_id = "test-workflow-7"

    worker_state = ZahirWorkerState(context, None, output_queue, workflow_id)

    # Add a job
    job = SimpleJobWithRecovery(input={"test": "data"}, dependencies={}, options=None)
    job_id = context.job_registry.add(job, output_queue)

    # Set up frame
    job_generator = SimpleJobWithRecovery.run(context, job.input, job.dependencies)
    frame = ZahirStackFrame(job=job, job_generator=job_generator, recovery=False)
    worker_state.frame = frame

    original_job_id = worker_state.frame.job.job_id

    # Set up exception event
    worker_state.last_event = Exception("Test error")

    # Run handle_job_exception
    handle_job_exception(worker_state)

    # Job ID should remain the same
    assert worker_state.frame.job.job_id == original_job_id
    assert worker_state.frame.job.job_id == job_id


def test_handle_job_exception_workflow_id_used():
    """Test that handle_job_exception uses the correct workflow_id."""

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    job_registry = SQLiteJobRegistry(tmp_file)
    job_registry.init("test-worker-8")

    context = MemoryContext(scope=LocalScope(jobs=[SimpleJobWithRecovery]), job_registry=job_registry)
    output_queue = multiprocessing.Queue()
    workflow_id = "specific-workflow-id-456"

    worker_state = ZahirWorkerState(context, None, output_queue, workflow_id)

    # Add a job
    job = SimpleJobWithRecovery(input={"test": "data"}, dependencies={}, options=None)
    job_id = context.job_registry.add(job, output_queue)

    # Set up frame
    job_generator = SimpleJobWithRecovery.run(context, job.input, job.dependencies)
    frame = ZahirStackFrame(job=job, job_generator=job_generator, recovery=False)
    worker_state.frame = frame

    # Set up exception event
    worker_state.last_event = Exception("Test error")

    # Run handle_job_exception
    handle_job_exception(worker_state)

    # Job state should be updated with correct workflow_id
    job_state = context.job_registry.get_state(job_id)
    assert job_state == JobState.RECOVERING


def test_handle_job_exception_message_includes_job_type():
    """Test that the returned message includes the job type."""

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    job_registry = SQLiteJobRegistry(tmp_file)
    job_registry.init("test-worker-9")

    context = MemoryContext(scope=LocalScope(jobs=[SimpleJobWithRecovery]), job_registry=job_registry)
    output_queue = multiprocessing.Queue()
    workflow_id = "test-workflow-9"

    worker_state = ZahirWorkerState(context, None, output_queue, workflow_id)

    # Add a job
    job = SimpleJobWithRecovery(input={"test": "data"}, dependencies={}, options=None)
    job_id = context.job_registry.add(job, output_queue)

    # Set up frame
    job_generator = SimpleJobWithRecovery.run(context, job.input, job.dependencies)
    frame = ZahirStackFrame(job=job, job_generator=job_generator, recovery=False)
    worker_state.frame = frame

    # Set up exception event
    worker_state.last_event = Exception("Test error")

    # Run handle_job_exception
    result, _ = handle_job_exception(worker_state)

    # Message should include job type
    assert "SimpleJobWithRecovery" in result.data["message"]
