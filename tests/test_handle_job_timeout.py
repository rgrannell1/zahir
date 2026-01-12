"""Tests for the handle_job_timeout state machine step.

Tests the handle_job_timeout function which handles when a normal job
exceeds its timeout limit.
"""

import multiprocessing
import tempfile

from zahir.base_types import Context, JobState
from zahir.context import MemoryContext
from zahir.events import JobOutputEvent
from zahir.exception import JobTimeoutError
from zahir.job_registry import SQLiteJobRegistry
from zahir.jobs.decorator import job
from zahir.scope import LocalScope
from zahir.worker.call_frame import ZahirStackFrame
from zahir.worker.state_machine import ZahirWorkerState
from zahir.worker.state_machine.handle_job_timeout import handle_job_timeout
from zahir.worker.state_machine.states import WaitForJobStateChange


@job()
def SimpleJob(context: Context, input, dependencies):
    """A simple job for testing."""
    yield JobOutputEvent({"result": "done"})


@job()
def AnotherJob(context: Context, input, dependencies):
    """Another job for testing."""
    yield JobOutputEvent({"count": 1})


def test_handle_job_timeout_returns_enqueue_state_change():
    """Test that handle_job_timeout returns EnqueueJobStateChange."""

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    job_registry = SQLiteJobRegistry(tmp_file)
    job_registry.init("test-worker-1")

    context = MemoryContext(scope=LocalScope(jobs=[SimpleJob]), job_registry=job_registry)
    input_queue = multiprocessing.Queue()
    output_queue = multiprocessing.Queue()
    workflow_id = "test-workflow-1"

    worker_state = ZahirWorkerState(context, input_queue, output_queue, workflow_id)

    # Add a job
    job = SimpleJob({"test": "data"}, {})
    job_id = context.job_registry.add(context, job, output_queue)

    # Create a normal (non-recovery) frame
    job_generator = SimpleJob.run(context, job.input, job.dependencies)
    worker_state.frame = ZahirStackFrame(job=job, job_generator=job_generator, recovery=False)

    # Run handle_job_timeout
    result, _ = handle_job_timeout(worker_state)

    # Verify result
    assert isinstance(result, WaitForJobStateChange)
    assert "timed out" in result.data["message"]
    assert "SimpleJob" in result.data["message"]


def test_handle_job_timeout_sets_timed_out_state():
    """Test that the job state is set to TIMED_OUT."""

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    job_registry = SQLiteJobRegistry(tmp_file)
    job_registry.init("test-worker-2")

    context = MemoryContext(scope=LocalScope(jobs=[SimpleJob]), job_registry=job_registry)
    input_queue = multiprocessing.Queue()
    output_queue = multiprocessing.Queue()
    workflow_id = "test-workflow-2"

    worker_state = ZahirWorkerState(context, input_queue, output_queue, workflow_id)

    # Add a job
    job = SimpleJob({"test": "data"}, {})
    job_id = context.job_registry.add(context, job, output_queue)

    # Create a normal frame
    job_generator = SimpleJob.run(context, job.input, job.dependencies)
    worker_state.frame = ZahirStackFrame(job=job, job_generator=job_generator, recovery=False)

    # Run handle_job_timeout
    handle_job_timeout(worker_state)

    # Verify job state
    job_state = context.job_registry.get_state(job.job_id)
    assert job_state == JobState.TIMED_OUT


def test_handle_job_timeout_records_error():
    """Test that a JobTimeoutError is recorded."""

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    job_registry = SQLiteJobRegistry(tmp_file)
    job_registry.init("test-worker-3")

    context = MemoryContext(scope=LocalScope(jobs=[SimpleJob]), job_registry=job_registry)
    input_queue = multiprocessing.Queue()
    output_queue = multiprocessing.Queue()
    workflow_id = "test-workflow-3"

    worker_state = ZahirWorkerState(context, input_queue, output_queue, workflow_id)

    # Add a job
    job = SimpleJob({"test": "data"}, {})
    job_id = context.job_registry.add(context, job, output_queue)

    # Create a normal frame
    job_generator = SimpleJob.run(context, job.input, job.dependencies)
    worker_state.frame = ZahirStackFrame(job=job, job_generator=job_generator, recovery=False)

    # Run handle_job_timeout
    handle_job_timeout(worker_state)

    # Verify error was recorded
    errors = context.job_registry.get_errors(job.job_id)
    assert len(errors) > 0
    assert isinstance(errors[-1], JobTimeoutError)
    assert "timed out" in str(errors[-1]).lower()


def test_handle_job_timeout_clears_frame():
    """Test that the active frame is cleared."""

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    job_registry = SQLiteJobRegistry(tmp_file)
    job_registry.init("test-worker-4")

    context = MemoryContext(scope=LocalScope(jobs=[SimpleJob]), job_registry=job_registry)
    input_queue = multiprocessing.Queue()
    output_queue = multiprocessing.Queue()
    workflow_id = "test-workflow-4"

    worker_state = ZahirWorkerState(context, input_queue, output_queue, workflow_id)

    # Add a job
    job = SimpleJob({"test": "data"}, {})
    job_id = context.job_registry.add(context, job, output_queue)

    # Create a normal frame
    job_generator = SimpleJob.run(context, job.input, job.dependencies)
    worker_state.frame = ZahirStackFrame(job=job, job_generator=job_generator, recovery=False)

    # Verify frame is set
    assert worker_state.frame is not None

    # Run handle_job_timeout
    handle_job_timeout(worker_state)

    # Verify frame is cleared
    assert worker_state.frame is None


def test_handle_job_timeout_preserves_state():
    """Test that handle_job_timeout returns the same state object."""

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    job_registry = SQLiteJobRegistry(tmp_file)
    job_registry.init("test-worker-5")

    context = MemoryContext(scope=LocalScope(jobs=[SimpleJob]), job_registry=job_registry)
    input_queue = multiprocessing.Queue()
    output_queue = multiprocessing.Queue()
    workflow_id = "test-workflow-5"

    worker_state = ZahirWorkerState(context, input_queue, output_queue, workflow_id)

    # Add a job
    job = SimpleJob({"test": "data"}, {})
    job_id = context.job_registry.add(context, job, output_queue)

    # Create a normal frame
    job_generator = SimpleJob.run(context, job.input, job.dependencies)
    worker_state.frame = ZahirStackFrame(job=job, job_generator=job_generator, recovery=False)

    # Run handle_job_timeout
    result, returned_state = handle_job_timeout(worker_state)

    # Verify same state object returned
    assert returned_state is worker_state


def test_handle_job_timeout_includes_job_type_in_message():
    """Test that the job type appears in the state change message."""

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    job_registry = SQLiteJobRegistry(tmp_file)
    job_registry.init("test-worker-6")

    context = MemoryContext(scope=LocalScope(jobs=[AnotherJob]), job_registry=job_registry)
    input_queue = multiprocessing.Queue()
    output_queue = multiprocessing.Queue()
    workflow_id = "test-workflow-6"

    worker_state = ZahirWorkerState(context, input_queue, output_queue, workflow_id)

    # Add a job
    job = AnotherJob({"count": 0}, {})
    job_id = context.job_registry.add(context, job, output_queue)

    # Create a normal frame
    job_generator = AnotherJob.run(context, job.input, job.dependencies)
    worker_state.frame = ZahirStackFrame(job=job, job_generator=job_generator, recovery=False)

    # Run handle_job_timeout
    result, _ = handle_job_timeout(worker_state)

    # Verify job type in message
    assert "AnotherJob" in result.data["message"]


def test_handle_job_timeout_transitions_to_enqueue():
    """Test that the state transitions to enqueue to start over."""

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    job_registry = SQLiteJobRegistry(tmp_file)
    job_registry.init("test-worker-7")

    context = MemoryContext(scope=LocalScope(jobs=[SimpleJob]), job_registry=job_registry)
    input_queue = multiprocessing.Queue()
    output_queue = multiprocessing.Queue()
    workflow_id = "test-workflow-7"

    worker_state = ZahirWorkerState(context, input_queue, output_queue, workflow_id)

    # Add a job
    job = SimpleJob({"test": "data"}, {})
    job_id = context.job_registry.add(context, job, output_queue)

    # Create a normal frame
    job_generator = SimpleJob.run(context, job.input, job.dependencies)
    worker_state.frame = ZahirStackFrame(job=job, job_generator=job_generator, recovery=False)

    # Run handle_job_timeout
    result, _ = handle_job_timeout(worker_state)

    # Verify transition to wait for job
    assert isinstance(result, WaitForJobStateChange)
    # Frame should be cleared to allow enqueueing next job
    assert worker_state.frame is None


def test_handle_job_timeout_error_message():
    """Test that the error message is correct."""

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    job_registry = SQLiteJobRegistry(tmp_file)
    job_registry.init("test-worker-8")

    context = MemoryContext(scope=LocalScope(jobs=[SimpleJob]), job_registry=job_registry)
    input_queue = multiprocessing.Queue()
    output_queue = multiprocessing.Queue()
    workflow_id = "test-workflow-8"

    worker_state = ZahirWorkerState(context, input_queue, output_queue, workflow_id)

    # Add a job
    job = SimpleJob({"test": "data"}, {})
    job_id = context.job_registry.add(context, job, output_queue)

    # Create a normal frame
    job_generator = SimpleJob.run(context, job.input, job.dependencies)
    worker_state.frame = ZahirStackFrame(job=job, job_generator=job_generator, recovery=False)

    # Run handle_job_timeout
    handle_job_timeout(worker_state)

    # Verify error message
    errors = context.job_registry.get_errors(job.job_id)
    assert len(errors) > 0
    error_msg = str(errors[-1])
    assert error_msg == "Job execution timed out"
