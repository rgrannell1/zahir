"""Tests for the enqueue_job state machine step.

Tests the enqueue_job function which claims jobs from the registry and pushes them onto the stack.
This is how new top-level jobs enter the worker's execution flow.
"""

import tempfile
import multiprocessing

from zahir.base_types import Context, Job
from zahir.context import MemoryContext
from zahir.events import JobOutputEvent
from zahir.job_registry import SQLiteJobRegistry
from zahir.scope import LocalScope
from zahir.jobs.decorator import job
from zahir.worker.state_machine import ZahirWorkerState
from zahir.worker.state_machine.enqueue_job import enqueue_job
from zahir.worker.state_machine.states import StartStateChange, WaitForJobStateChange


@job
def SimpleJob(context: Context, input, dependencies):
    """A simple job that yields output."""
    yield JobOutputEvent({"result": "done"})


@job
def AnotherJob(context: Context, input, dependencies):
    """Another simple job for testing multiple jobs."""
    yield JobOutputEvent({"count": input.get("count", 0) + 1})


def test_enqueue_job_claims_pending_job():
    """Test that enqueue_job claims a READY job and transitions to StartStateChange."""

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    job_registry = SQLiteJobRegistry(tmp_file)
    job_registry.init("test-worker-1")

    context = MemoryContext(scope=LocalScope(jobs=[SimpleJob]), job_registry=job_registry)
    output_queue = multiprocessing.Queue()
    workflow_id = "test-workflow-1"

    worker_state = ZahirWorkerState(context, output_queue, workflow_id)

    # Add a job to the registry (no dependencies, so it will be READY)
    job = SimpleJob({"test": "data"}, {})
    job_id = context.job_registry.add(job, output_queue)

    # Verify stack is empty initially
    assert worker_state.job_stack.is_empty()

    # Run enqueue_job
    result, _ = enqueue_job(worker_state)

    # Verify result
    assert isinstance(result, StartStateChange)
    assert "Appended new job to stack" in result.data["message"]

    # Verify job was pushed onto stack
    assert not worker_state.job_stack.is_empty()
    frame = worker_state.job_stack.top()
    assert frame is not None
    assert frame.job.job_id == job_id
    assert frame.recovery is False
    assert len(frame.required_jobs) == 0


def test_enqueue_job_no_pending_jobs():
    """Test that enqueue_job returns WaitForJobStateChange when no jobs are available."""

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    job_registry = SQLiteJobRegistry(tmp_file)
    job_registry.init("test-worker-2")

    context = MemoryContext(scope=LocalScope(jobs=[SimpleJob]), job_registry=job_registry)
    output_queue = multiprocessing.Queue()
    workflow_id = "test-workflow-2"

    worker_state = ZahirWorkerState(context, output_queue, workflow_id)

    # Don't add any jobs - registry is empty

    # Run enqueue_job
    result, _ = enqueue_job(worker_state)

    # Verify result
    assert isinstance(result, WaitForJobStateChange)
    assert "no pending job to claim" in result.data["message"]

    # Verify stack remains empty
    assert worker_state.job_stack.is_empty()


def test_enqueue_job_multiple_jobs_available():
    """Test that enqueue_job claims the oldest job when multiple are available."""

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    job_registry = SQLiteJobRegistry(tmp_file)
    job_registry.init("test-worker-3")

    context = MemoryContext(scope=LocalScope(jobs=[SimpleJob, AnotherJob]), job_registry=job_registry)
    output_queue = multiprocessing.Queue()
    workflow_id = "test-workflow-3"

    worker_state = ZahirWorkerState(context, output_queue, workflow_id)

    # Add multiple jobs in sequence
    job1 = SimpleJob({"order": 1}, {})
    job_id1 = context.job_registry.add(job1, output_queue)

    import time

    time.sleep(0.01)  # Ensure different timestamps

    job2 = AnotherJob({"order": 2, "count": 0}, {})
    job_id2 = context.job_registry.add(job2, output_queue)

    # Run enqueue_job
    result, _ = enqueue_job(worker_state)

    # Verify we got the first job (oldest)
    assert isinstance(result, StartStateChange)
    frame = worker_state.job_stack.top()
    assert frame is not None
    assert frame.job.job_id == job_id1  # First job should be claimed


def test_enqueue_job_creates_generator():
    """Test that enqueue_job creates a job generator for the claimed job."""

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    job_registry = SQLiteJobRegistry(tmp_file)
    job_registry.init("test-worker-4")

    context = MemoryContext(scope=LocalScope(jobs=[SimpleJob]), job_registry=job_registry)
    output_queue = multiprocessing.Queue()
    workflow_id = "test-workflow-4"

    worker_state = ZahirWorkerState(context, output_queue, workflow_id)

    # Add a job
    job = SimpleJob({"test": "data"}, {})
    job_id = context.job_registry.add(job, output_queue)

    # Run enqueue_job
    result, _ = enqueue_job(worker_state)

    # Verify generator was created
    frame = worker_state.job_stack.top()
    assert frame is not None
    assert frame.job_generator is not None

    # Verify it's actually a generator we can iterate
    import types

    assert isinstance(frame.job_generator, types.GeneratorType)


def test_enqueue_job_stack_push():
    """Test that enqueue_job correctly pushes the frame onto the stack."""

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    job_registry = SQLiteJobRegistry(tmp_file)
    job_registry.init("test-worker-5")

    context = MemoryContext(scope=LocalScope(jobs=[SimpleJob]), job_registry=job_registry)
    output_queue = multiprocessing.Queue()
    workflow_id = "test-workflow-5"

    worker_state = ZahirWorkerState(context, output_queue, workflow_id)

    # Add a job
    job = SimpleJob({"test": "data"}, {})
    job_id = context.job_registry.add(job, output_queue)

    # Verify stack depth before
    initial_depth = len(worker_state.job_stack.frames)
    assert initial_depth == 0

    # Run enqueue_job
    enqueue_job(worker_state)

    # Verify stack depth after
    assert len(worker_state.job_stack.frames) == initial_depth + 1


def test_enqueue_job_frame_not_recovery():
    """Test that enqueued jobs are not marked as recovery jobs."""

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    job_registry = SQLiteJobRegistry(tmp_file)
    job_registry.init("test-worker-6")

    context = MemoryContext(scope=LocalScope(jobs=[SimpleJob]), job_registry=job_registry)
    output_queue = multiprocessing.Queue()
    workflow_id = "test-workflow-6"

    worker_state = ZahirWorkerState(context, output_queue, workflow_id)

    # Add a job
    job = SimpleJob({"test": "data"}, {})
    job_id = context.job_registry.add(job, output_queue)

    # Run enqueue_job
    enqueue_job(worker_state)

    # Verify frame is not marked as recovery
    frame = worker_state.job_stack.top()
    assert frame is not None
    assert frame.recovery is False


def test_enqueue_job_frame_no_required_jobs():
    """Test that newly enqueued top-level jobs have no required jobs."""

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    job_registry = SQLiteJobRegistry(tmp_file)
    job_registry.init("test-worker-7")

    context = MemoryContext(scope=LocalScope(jobs=[SimpleJob]), job_registry=job_registry)
    output_queue = multiprocessing.Queue()
    workflow_id = "test-workflow-7"

    worker_state = ZahirWorkerState(context, output_queue, workflow_id)

    # Add a job
    job = SimpleJob({"test": "data"}, {})
    job_id = context.job_registry.add(job, output_queue)

    # Run enqueue_job
    enqueue_job(worker_state)

    # Verify frame has no required jobs
    frame = worker_state.job_stack.top()
    assert frame is not None
    assert len(frame.required_jobs) == 0
    assert frame.await_many is False


def test_enqueue_job_preserves_job_input():
    """Test that the enqueued job preserves its input data."""

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    job_registry = SQLiteJobRegistry(tmp_file)
    job_registry.init("test-worker-8")

    context = MemoryContext(scope=LocalScope(jobs=[AnotherJob]), job_registry=job_registry)
    output_queue = multiprocessing.Queue()
    workflow_id = "test-workflow-8"

    worker_state = ZahirWorkerState(context, output_queue, workflow_id)

    # Add a job with specific input
    expected_input = {"count": 42, "name": "test"}
    job = AnotherJob(expected_input, {})
    job_id = context.job_registry.add(job, output_queue)

    # Run enqueue_job
    enqueue_job(worker_state)

    # Verify the job input is preserved
    frame = worker_state.job_stack.top()
    assert frame is not None
    assert frame.job.input == expected_input


def test_enqueue_job_worker_isolation():
    """Test that different workers can claim different jobs."""

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    job_registry = SQLiteJobRegistry(tmp_file)
    job_registry.init("test-worker-9a")

    context = MemoryContext(scope=LocalScope(jobs=[SimpleJob]), job_registry=job_registry)
    output_queue = multiprocessing.Queue()
    workflow_id = "test-workflow-9"

    # Add two jobs
    job1 = SimpleJob({"worker": 1}, {})
    job_id1 = context.job_registry.add(job1, output_queue)

    job2 = SimpleJob({"worker": 2}, {})
    job_id2 = context.job_registry.add(job2, output_queue)

    # First worker claims a job
    worker_state1 = ZahirWorkerState(context, output_queue, workflow_id)
    result1, _ = enqueue_job(worker_state1)
    assert isinstance(result1, StartStateChange)
    claimed_id1 = worker_state1.job_stack.top().job.job_id

    # Second worker should claim the other job
    worker_state2 = ZahirWorkerState(context, output_queue, workflow_id)
    result2, _ = enqueue_job(worker_state2)
    assert isinstance(result2, StartStateChange)
    claimed_id2 = worker_state2.job_stack.top().job.job_id

    # Verify different jobs were claimed
    assert claimed_id1 != claimed_id2
    assert {claimed_id1, claimed_id2} == {job_id1, job_id2}
