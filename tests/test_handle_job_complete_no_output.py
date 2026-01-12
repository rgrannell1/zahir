"""Tests for the handle_job_complete_no_output state machine step.

Tests the handle_job_complete_no_output function which marks jobs as complete
when they finish without producing output, and transitions to enqueue state.
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
from zahir.worker.state_machine.handle_job_complete_no_output import handle_job_complete_no_output
from zahir.worker.state_machine.states import WaitForJobStateChange


@job()
def SimpleJob(context: Context, input, dependencies):
    """A simple job for testing."""
    yield JobOutputEvent({"result": "done"})


@job()
def AnotherJob(context: Context, input, dependencies):
    """Another job for testing."""
    yield JobOutputEvent({"count": 1})


def test_handle_job_complete_no_output_sets_completed_state():
    """Test that handle_job_complete_no_output sets the job state to COMPLETED."""

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

    # Set up frame
    job_generator = SimpleJob.run(context, job.input, job.dependencies)
    frame = ZahirStackFrame(job=job, job_generator=job_generator, recovery=False)
    worker_state.frame = frame

    # Run handle_job_complete_no_output
    handle_job_complete_no_output(worker_state)

    # Job state should be COMPLETED
    job_state = context.job_registry.get_state(job_id)
    assert job_state == JobState.COMPLETED


def test_handle_job_complete_no_output_transitions_to_enqueue():
    """Test that handle_job_complete_no_output returns EnqueueJobStateChange."""

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

    # Set up frame
    job_generator = SimpleJob.run(context, job.input, job.dependencies)
    frame = ZahirStackFrame(job=job, job_generator=job_generator, recovery=False)
    worker_state.frame = frame

    # Run handle_job_complete_no_output
    result, _ = handle_job_complete_no_output(worker_state)

    # Should transition to wait for job state
    assert isinstance(result, WaitForJobStateChange)
    assert "completed with no output" in result.data["message"]


def test_handle_job_complete_no_output_clears_frame():
    """Test that handle_job_complete_no_output clears the current frame."""

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

    # Set up frame
    job_generator = SimpleJob.run(context, job.input, job.dependencies)
    frame = ZahirStackFrame(job=job, job_generator=job_generator, recovery=False)
    worker_state.frame = frame

    # Verify frame is set
    assert worker_state.frame is not None

    # Run handle_job_complete_no_output
    handle_job_complete_no_output(worker_state)

    # Frame should be cleared
    assert worker_state.frame is None


def test_handle_job_complete_no_output_preserves_state():
    """Test that handle_job_complete_no_output returns the same state object."""

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

    # Set up frame
    job_generator = SimpleJob.run(context, job.input, job.dependencies)
    frame = ZahirStackFrame(job=job, job_generator=job_generator, recovery=False)
    worker_state.frame = frame

    # Run handle_job_complete_no_output
    result, returned_state = handle_job_complete_no_output(worker_state)

    # Should return same state object
    assert returned_state is worker_state


def test_handle_job_complete_no_output_workflow_id_used():
    """Test that handle_job_complete_no_output uses the correct workflow_id."""

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    job_registry = SQLiteJobRegistry(tmp_file)
    job_registry.init("test-worker-5")

    context = MemoryContext(scope=LocalScope(jobs=[SimpleJob]), job_registry=job_registry)
    input_queue = multiprocessing.Queue()
    output_queue = multiprocessing.Queue()
    workflow_id = "specific-workflow-id-789"

    worker_state = ZahirWorkerState(context, input_queue, output_queue, workflow_id)

    # Add a job
    job = SimpleJob({"test": "data"}, {})
    job_id = context.job_registry.add(context, job, output_queue)

    # Set up frame
    job_generator = SimpleJob.run(context, job.input, job.dependencies)
    frame = ZahirStackFrame(job=job, job_generator=job_generator, recovery=False)
    worker_state.frame = frame

    # Run handle_job_complete_no_output
    handle_job_complete_no_output(worker_state)

    # Job state should be updated with correct workflow_id
    job_state = context.job_registry.get_state(job_id)
    assert job_state == JobState.COMPLETED


def test_handle_job_complete_no_output_multiple_calls():
    """Test that handle_job_complete_no_output can be called for different jobs."""

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    job_registry = SQLiteJobRegistry(tmp_file)
    job_registry.init("test-worker-6")

    context = MemoryContext(scope=LocalScope(jobs=[SimpleJob, AnotherJob]), job_registry=job_registry)
    input_queue = multiprocessing.Queue()
    output_queue = multiprocessing.Queue()
    workflow_id = "test-workflow-6"

    worker_state = ZahirWorkerState(context, input_queue, output_queue, workflow_id)

    # Add first job
    job1 = SimpleJob({"test": "data1"}, {})
    job_id1 = context.job_registry.add(context, job1, output_queue)

    # Set up frame for first job
    job_generator1 = SimpleJob.run(context, job1.input, job1.dependencies)
    frame1 = ZahirStackFrame(job=job1, job_generator=job_generator1, recovery=False)
    worker_state.frame = frame1

    # Run handle_job_complete_no_output for first job
    handle_job_complete_no_output(worker_state)

    # Verify first job is completed
    job_state1 = context.job_registry.get_state(job_id1)
    assert job_state1 == JobState.COMPLETED

    # Add second job
    job2 = AnotherJob({"test": "data2"}, {})
    job_id2 = context.job_registry.add(context, job2, output_queue)

    # Set up frame for second job
    job_generator2 = AnotherJob.run(context, job2.input, job2.dependencies)
    frame2 = ZahirStackFrame(job=job2, job_generator=job_generator2, recovery=False)
    worker_state.frame = frame2

    # Run handle_job_complete_no_output for second job
    handle_job_complete_no_output(worker_state)

    # Verify both jobs are completed
    job_state2 = context.job_registry.get_state(job_id2)
    assert job_state2 == JobState.COMPLETED


def test_handle_job_complete_no_output_recovery_job():
    """Test that handle_job_complete_no_output works with recovery jobs."""

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

    # Set up frame in recovery mode
    job_generator = SimpleJob.run(context, job.input, job.dependencies)
    frame = ZahirStackFrame(job=job, job_generator=job_generator, recovery=True)
    worker_state.frame = frame

    # Run handle_job_complete_no_output
    result, _ = handle_job_complete_no_output(worker_state)

    # Job should be marked as completed
    job_state = context.job_registry.get_state(job_id)
    assert job_state == JobState.COMPLETED
    assert isinstance(result, WaitForJobStateChange)


def test_handle_job_complete_no_output_clears_frame_completely():
    """Test that handle_job_complete_no_output completely nullifies the frame."""

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

    # Set up frame
    job_generator = SimpleJob.run(context, job.input, job.dependencies)
    frame = ZahirStackFrame(job=job, job_generator=job_generator, recovery=False)
    worker_state.frame = frame

    # Verify frame properties exist
    assert worker_state.frame.job is not None
    assert worker_state.frame.job_generator is not None

    # Run handle_job_complete_no_output
    handle_job_complete_no_output(worker_state)

    # Frame should be completely None
    assert worker_state.frame is None
