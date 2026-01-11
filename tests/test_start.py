"""Tests for the start state machine step.

Tests the start function which is the initial state that determines
what to do next: check preconditions, enqueue a job, or pop from stack.
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
from zahir.worker.state_machine.start import start
from zahir.worker.state_machine.states import (
    CheckPreconditionsStateChange,
    PopJobStateChange,
    WaitForJobStateChange,
)


@job()
def SimpleJob(context: Context, input, dependencies):
    """A simple job for testing."""
    yield JobOutputEvent({"result": "done"})


@job()
def AnotherJob(context: Context, input, dependencies):
    """Another job for testing."""
    yield JobOutputEvent({"count": 1})


def test_start_no_frame_empty_stack_enqueues():
    """Test that start enqueues when there's no frame and stack is empty."""

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    job_registry = SQLiteJobRegistry(tmp_file)
    job_registry.init("test-worker-1")

    context = MemoryContext(scope=LocalScope(jobs=[SimpleJob]), job_registry=job_registry)
    input_queue = multiprocessing.Queue()
    output_queue = multiprocessing.Queue()
    workflow_id = "test-workflow-1"

    worker_state = ZahirWorkerState(context, input_queue, output_queue, workflow_id)

    # Verify no frame and empty stack
    assert worker_state.frame is None
    assert worker_state.job_stack.is_empty()

    # Run start
    result, _ = start(worker_state)

    # Should wait for job
    assert isinstance(result, WaitForJobStateChange)
    assert result.data["message"] == "No job; waiting for dispatch."


def test_start_no_frame_with_runnable_job_pops():
    """Test that start pops when there's a runnable job on the stack."""

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    job_registry = SQLiteJobRegistry(tmp_file)
    job_registry.init("test-worker-2")

    context = MemoryContext(scope=LocalScope(jobs=[SimpleJob]), job_registry=job_registry)
    input_queue = multiprocessing.Queue()
    output_queue = multiprocessing.Queue()
    workflow_id = "test-workflow-2"

    worker_state = ZahirWorkerState(context, input_queue, output_queue, workflow_id)

    # Add a job to the registry in READY state
    job = SimpleJob({"test": "data"}, {})
    job_id = context.job_registry.add(job, output_queue)

    # Create a frame and push it to the stack
    job_generator = SimpleJob.run(context, job.input, job.dependencies)
    frame = ZahirStackFrame(job=job, job_generator=job_generator, recovery=False)
    worker_state.job_stack.push(frame)

    # Verify no active frame but stack is not empty
    assert worker_state.frame is None
    assert not worker_state.job_stack.is_empty()

    # Run start
    result, _ = start(worker_state)

    # Should pop
    assert isinstance(result, PopJobStateChange)
    assert result.data["message"] == "No job active, so popping from stack"


def test_start_no_frame_with_paused_job_enqueues():
    """Test that start enqueues when job on stack is paused (not runnable)."""

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    job_registry = SQLiteJobRegistry(tmp_file)
    job_registry.init("test-worker-3")

    context = MemoryContext(scope=LocalScope(jobs=[SimpleJob, AnotherJob]), job_registry=job_registry)
    input_queue = multiprocessing.Queue()
    output_queue = multiprocessing.Queue()
    workflow_id = "test-workflow-3"

    worker_state = ZahirWorkerState(context, input_queue, output_queue, workflow_id)

    # Add a job and set it to PAUSED
    job = SimpleJob({"test": "data"}, {})
    job_id = context.job_registry.add(job, output_queue)
    context.job_registry.set_state(job.job_id, workflow_id, output_queue, JobState.PAUSED)

    # Add another job that the first job is waiting for (not finished yet)
    another_job = AnotherJob({"count": 0}, {})
    required_job_id = context.job_registry.add(another_job, output_queue)
    context.job_registry.set_state(required_job_id, workflow_id, output_queue, JobState.RUNNING)

    # Create a frame that requires another job (so it's waiting)
    job_generator = SimpleJob.run(context, job.input, job.dependencies)
    frame = ZahirStackFrame(job=job, job_generator=job_generator, recovery=False)
    frame.required_jobs.add(required_job_id)  # Make it require the other job
    worker_state.job_stack.push(frame)

    # Verify no active frame but stack is not empty
    assert worker_state.frame is None
    assert not worker_state.job_stack.is_empty()

    # Run start
    result, _ = start(worker_state)

    # Should wait for job since job is not runnable (waiting for another job)
    assert isinstance(result, WaitForJobStateChange)
    assert result.data["message"] == "No job runnable; waiting for dispatch."


def test_start_with_active_frame_checks_preconditions():
    """Test that start checks preconditions when there's an active frame."""

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
    job_id = context.job_registry.add(job, output_queue)

    # Create and set an active frame
    job_generator = SimpleJob.run(context, job.input, job.dependencies)
    worker_state.frame = ZahirStackFrame(job=job, job_generator=job_generator, recovery=False)

    # Run start
    result, _ = start(worker_state)

    # Should check preconditions
    assert isinstance(result, CheckPreconditionsStateChange)
    assert "SimpleJob" in result.data["message"]
    assert "Checking preconditions" in result.data["message"]


def test_start_preserves_state():
    """Test that start returns the same state object unchanged."""

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    job_registry = SQLiteJobRegistry(tmp_file)
    job_registry.init("test-worker-5")

    context = MemoryContext(scope=LocalScope(jobs=[SimpleJob]), job_registry=job_registry)
    input_queue = multiprocessing.Queue()
    output_queue = multiprocessing.Queue()
    workflow_id = "test-workflow-5"

    worker_state = ZahirWorkerState(context, input_queue, output_queue, workflow_id)

    # Run start
    result, returned_state = start(worker_state)

    # State should be unchanged
    assert returned_state is worker_state


def test_start_job_type_in_message():
    """Test that the job type appears correctly in the check preconditions message."""

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
    job_id = context.job_registry.add(job, output_queue)

    # Create and set an active frame
    job_generator = AnotherJob.run(context, job.input, job.dependencies)
    worker_state.frame = ZahirStackFrame(job=job, job_generator=job_generator, recovery=False)

    # Run start
    result, _ = start(worker_state)

    # Verify job type in message
    assert isinstance(result, CheckPreconditionsStateChange)
    assert "AnotherJob" in result.data["message"]


def test_start_empty_stack_to_enqueue_transition():
    """Test the specific transition from empty stack to enqueue."""

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    job_registry = SQLiteJobRegistry(tmp_file)
    job_registry.init("test-worker-7")

    context = MemoryContext(scope=LocalScope(jobs=[]), job_registry=job_registry)
    input_queue = multiprocessing.Queue()
    output_queue = multiprocessing.Queue()
    workflow_id = "test-workflow-7"

    worker_state = ZahirWorkerState(context, input_queue, output_queue, workflow_id)

    # Start conditions: no frame, empty stack
    assert worker_state.frame is None
    assert worker_state.job_stack.is_empty()

    result, _ = start(worker_state)

    # Should transition to wait for job
    assert isinstance(result, WaitForJobStateChange)
    # State should remain unchanged
    assert worker_state.frame is None
    assert worker_state.job_stack.is_empty()


def test_start_with_recovery_frame():
    """Test that start handles recovery frames correctly."""

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
    job_id = context.job_registry.add(job, output_queue)

    # Create and set an active recovery frame
    job_generator = SimpleJob.recover(context, job.input, job.dependencies, None)
    worker_state.frame = ZahirStackFrame(job=job, job_generator=job_generator, recovery=True)

    # Run start
    result, _ = start(worker_state)

    # Should still check preconditions for recovery frame
    assert isinstance(result, CheckPreconditionsStateChange)
    assert "SimpleJob" in result.data["message"]
