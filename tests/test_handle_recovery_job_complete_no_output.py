"""Tests for the handle_recovery_job_complete_no_output state machine step.

Tests the handle_recovery_job_complete_no_output function which handles when a
recovery job completes successfully without producing output.
"""

import multiprocessing
import tempfile

from zahir.base_types import Context, JobState
from zahir.context import MemoryContext
from zahir.events import JobOutputEvent
from zahir.job_registry import SQLiteJobRegistry
from zahir.jobs.decorator import job
from zahir.scope import LocalScope
from zahir.worker.call_frame import ZahirStackFrame
from zahir.worker.state_machine import ZahirWorkerState
from zahir.worker.state_machine.handle_recovery_job_complete_no_output import (
    handle_recovery_job_complete_no_output,
)
from zahir.worker.state_machine.states import WaitForJobStateChange


@job()
def SimpleJob(context: Context, input, dependencies):
    """A simple job for testing."""
    yield JobOutputEvent({"result": "done"})


@job()
def AnotherJob(context: Context, input, dependencies):
    """Another job for testing."""
    yield JobOutputEvent({"count": 1})


def test_handle_recovery_job_complete_no_output_returns_enqueue_state_change():
    """Test that handle_recovery_job_complete_no_output returns EnqueueJobStateChange."""

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

    # Create a recovery frame
    job_generator = SimpleJob.recover(context, job.input, job.dependencies, None)
    worker_state.frame = ZahirStackFrame(job=job, job_generator=job_generator, recovery=True)

    # Run handle_recovery_job_complete_no_output
    result, _ = handle_recovery_job_complete_no_output(worker_state)

    # Verify result
    assert isinstance(result, WaitForJobStateChange)
    assert result.data["message"] == "Recovery job completed with no output"


def test_handle_recovery_job_complete_no_output_sets_recovered_state():
    """Test that the job state is set to RECOVERED."""

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

    # Create a recovery frame
    job_generator = SimpleJob.recover(context, job.input, job.dependencies, None)
    worker_state.frame = ZahirStackFrame(job=job, job_generator=job_generator, recovery=True)

    # Run handle_recovery_job_complete_no_output
    handle_recovery_job_complete_no_output(worker_state)

    # Verify job state
    job_state = context.job_registry.get_state(job.job_id)
    assert job_state == JobState.RECOVERED


def test_handle_recovery_job_complete_no_output_clears_frame():
    """Test that the active frame is cleared."""

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

    # Create a recovery frame
    job_generator = SimpleJob.recover(context, job.input, job.dependencies, None)
    worker_state.frame = ZahirStackFrame(job=job, job_generator=job_generator, recovery=True)

    # Verify frame is set
    assert worker_state.frame is not None

    # Run handle_recovery_job_complete_no_output
    handle_recovery_job_complete_no_output(worker_state)

    # Verify frame is cleared
    assert worker_state.frame is None


def test_handle_recovery_job_complete_no_output_preserves_state():
    """Test that handle_recovery_job_complete_no_output returns the same state object."""

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

    # Create a recovery frame
    job_generator = SimpleJob.recover(context, job.input, job.dependencies, None)
    worker_state.frame = ZahirStackFrame(job=job, job_generator=job_generator, recovery=True)

    # Run handle_recovery_job_complete_no_output
    result, returned_state = handle_recovery_job_complete_no_output(worker_state)

    # Verify same state object returned
    assert returned_state is worker_state


def test_handle_recovery_job_complete_no_output_transitions_to_enqueue():
    """Test that the state transitions to enqueue to continue with next job."""

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

    # Create a recovery frame
    job_generator = SimpleJob.recover(context, job.input, job.dependencies, None)
    worker_state.frame = ZahirStackFrame(job=job, job_generator=job_generator, recovery=True)

    # Run handle_recovery_job_complete_no_output
    result, _ = handle_recovery_job_complete_no_output(worker_state)

    # Verify transition to wait for job
    assert isinstance(result, WaitForJobStateChange)
    # Frame should be cleared to allow enqueueing next job
    assert worker_state.frame is None


def test_handle_recovery_job_complete_no_output_with_different_jobs():
    """Test that different job types are handled correctly."""

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    job_registry = SQLiteJobRegistry(tmp_file)
    job_registry.init("test-worker-6")

    context = MemoryContext(scope=LocalScope(jobs=[SimpleJob, AnotherJob]), job_registry=job_registry)
    input_queue = multiprocessing.Queue()
    output_queue = multiprocessing.Queue()
    workflow_id = "test-workflow-6"

    # Test with SimpleJob
    worker_state = ZahirWorkerState(context, input_queue, output_queue, workflow_id)
    job1 = SimpleJob({"test": "data1"}, {})
    job_id1 = context.job_registry.add(context, job1, output_queue)
    job_generator1 = SimpleJob.recover(context, job1.input, job1.dependencies, None)
    worker_state.frame = ZahirStackFrame(job=job1, job_generator=job_generator1, recovery=True)

    handle_recovery_job_complete_no_output(worker_state)
    assert context.job_registry.get_state(job_id1) == JobState.RECOVERED

    # Test with AnotherJob
    worker_state = ZahirWorkerState(context, input_queue, output_queue, workflow_id)
    job2 = AnotherJob({"count": 0}, {})
    job_id2 = context.job_registry.add(context, job2, output_queue)
    job_generator2 = AnotherJob.recover(context, job2.input, job2.dependencies, None)
    worker_state.frame = ZahirStackFrame(job=job2, job_generator=job_generator2, recovery=True)

    handle_recovery_job_complete_no_output(worker_state)
    assert context.job_registry.get_state(job_id2) == JobState.RECOVERED


def test_handle_recovery_job_complete_no_output_message_content():
    """Test that the message is correct and consistent."""

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

    # Create a recovery frame
    job_generator = SimpleJob.recover(context, job.input, job.dependencies, None)
    worker_state.frame = ZahirStackFrame(job=job, job_generator=job_generator, recovery=True)

    # Run handle_recovery_job_complete_no_output
    result, _ = handle_recovery_job_complete_no_output(worker_state)

    # Verify message content
    assert "Recovery job completed" in result.data["message"]
    assert "no output" in result.data["message"]


def test_handle_recovery_job_complete_no_output_recovery_flag():
    """Test that the function works specifically for recovery jobs."""

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

    # Create a recovery frame (recovery=True)
    job_generator = SimpleJob.recover(context, job.input, job.dependencies, None)
    worker_state.frame = ZahirStackFrame(job=job, job_generator=job_generator, recovery=True)

    # Verify it's a recovery frame
    assert worker_state.frame.recovery is True

    # Run handle_recovery_job_complete_no_output
    result, _ = handle_recovery_job_complete_no_output(worker_state)

    # Verify job is marked as recovered
    job_state = context.job_registry.get_state(job.job_id)
    assert job_state == JobState.RECOVERED
