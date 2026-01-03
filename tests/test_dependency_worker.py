"""Tests for the dependency worker.

Tests the zahir_dependency_worker function which analyzes job dependencies
and transitions jobs from PENDING to READY when dependencies are satisfied,
or to IMPOSSIBLE when dependencies cannot be satisfied.
"""

import datetime
import multiprocessing
import tempfile
import time

from zahir.base_types import Context, JobState
from zahir.context import MemoryContext
from zahir.dependencies.job import JobDependency
from zahir.dependencies.time import TimeDependency
from zahir.events import JobOutputEvent, WorkflowCompleteEvent, ZahirInternalErrorEvent
from zahir.job_registry import SQLiteJobRegistry
from zahir.jobs.decorator import job
from zahir.scope import LocalScope
from zahir.worker.dependency_worker import zahir_dependency_worker


@job
def SimpleJob(context: Context, input, dependencies):
    """A simple job for testing."""
    yield JobOutputEvent({"result": "done"})


@job
def JobWithOutput(context: Context, input, dependencies):
    """A job that produces output."""
    yield JobOutputEvent({"count": input.get("count", 0) + 1})


def test_dependency_worker_marks_ready_when_satisfied():
    """Test that dependency worker marks PENDING jobs as READY when dependencies are satisfied."""

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    job_registry = SQLiteJobRegistry(tmp_file)
    job_registry.init("test-worker-1")

    # Create a time dependency that is already satisfied (in the past)
    past_time = datetime.datetime(2020, 1, 1, 0, 0, 0, tzinfo=datetime.UTC)
    satisfied_dependency = TimeDependency(after=past_time)

    context = MemoryContext(
        scope=LocalScope(jobs=[SimpleJob], dependencies=[TimeDependency]), job_registry=job_registry
    )
    output_queue = multiprocessing.Queue()
    workflow_id = "test-workflow-1"

    # Add a job with satisfied dependency
    job = SimpleJob({"test": "data"}, {"time_dep": satisfied_dependency})
    job_id = context.job_registry.add(job, output_queue)

    # Job should initially be PENDING
    assert context.job_registry.get_state(job_id) == JobState.PENDING

    # Start dependency worker in a separate process
    worker_process = multiprocessing.Process(target=zahir_dependency_worker, args=(context, output_queue, workflow_id))
    worker_process.start()

    # Wait a bit for dependency worker to process
    time.sleep(2)

    # Job should now be READY since dependency is satisfied
    assert context.job_registry.get_state(job_id) == JobState.READY

    # Mark job as completed to stop the worker
    context.job_registry.set_state(job_id, workflow_id, output_queue, JobState.COMPLETED)

    # Wait for worker to finish
    worker_process.join(timeout=3)
    if worker_process.is_alive():
        worker_process.terminate()


def test_dependency_worker_marks_impossible_when_unsatisfiable():
    """Test that dependency worker marks PENDING jobs as IMPOSSIBLE when dependencies cannot be satisfied."""

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    job_registry = SQLiteJobRegistry(tmp_file)
    job_registry.init("test-worker-2")

    # Create a time dependency that can never be satisfied (date in the past with 'before')
    past_time = datetime.datetime(2020, 1, 1, 0, 0, 0, tzinfo=datetime.UTC)
    impossible_dependency = TimeDependency(before=past_time)

    context = MemoryContext(
        scope=LocalScope(jobs=[SimpleJob], dependencies=[TimeDependency]), job_registry=job_registry
    )
    output_queue = multiprocessing.Queue()
    workflow_id = "test-workflow-2"

    # Add a job with impossible dependency
    job = SimpleJob({"test": "data"}, {"time_dep": impossible_dependency})
    job_id = context.job_registry.add(job, output_queue)

    # Job should initially be PENDING
    assert context.job_registry.get_state(job_id) == JobState.PENDING

    # Start dependency worker in a separate process
    worker_process = multiprocessing.Process(target=zahir_dependency_worker, args=(context, output_queue, workflow_id))
    worker_process.start()

    # Wait a bit for dependency worker to process
    time.sleep(2)

    # Job should now be IMPOSSIBLE since dependency cannot be satisfied
    assert context.job_registry.get_state(job_id) == JobState.IMPOSSIBLE

    # Worker should stop on its own since no jobs are active
    worker_process.join(timeout=3)
    if worker_process.is_alive():
        worker_process.terminate()


def test_dependency_worker_emits_workflow_complete():
    """Test that dependency worker emits WorkflowCompleteEvent when no jobs are active."""

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    job_registry = SQLiteJobRegistry(tmp_file)
    job_registry.init("test-worker-3")

    context = MemoryContext(scope=LocalScope(jobs=[SimpleJob]), job_registry=job_registry)
    output_queue = multiprocessing.Queue()
    workflow_id = "test-workflow-3"

    # Don't add any jobs - worker should complete immediately

    # Start dependency worker in a separate process
    worker_process = multiprocessing.Process(target=zahir_dependency_worker, args=(context, output_queue, workflow_id))
    worker_process.start()

    # Wait for worker to finish
    worker_process.join(timeout=3)
    if worker_process.is_alive():
        worker_process.terminate()

    # Check that WorkflowCompleteEvent was emitted
    event = output_queue.get(timeout=1)
    assert isinstance(event, WorkflowCompleteEvent)
    assert event.workflow_id == workflow_id


def test_dependency_worker_handles_job_dependency():
    """Test that dependency worker handles JobDependency correctly."""

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    job_registry = SQLiteJobRegistry(tmp_file)
    job_registry.init("test-worker-4")

    context = MemoryContext(
        scope=LocalScope(jobs=[SimpleJob, JobWithOutput], dependencies=[JobDependency]), job_registry=job_registry
    )
    output_queue = multiprocessing.Queue()
    workflow_id = "test-workflow-4"

    # Add first job that will be completed
    job1 = SimpleJob({"test": "data"}, {})
    job_id1 = context.job_registry.add(job1, output_queue)
    context.job_registry.set_state(job_id1, workflow_id, output_queue, JobState.COMPLETED)

    # Add second job that depends on the first
    job_dependency = JobDependency(job_id1, context.job_registry)
    job2 = JobWithOutput({"count": 0}, {"job_dep": job_dependency})
    job_id2 = context.job_registry.add(job2, output_queue)

    # Job 2 should initially be PENDING
    assert context.job_registry.get_state(job_id2) == JobState.PENDING

    # Start dependency worker in a separate process
    worker_process = multiprocessing.Process(target=zahir_dependency_worker, args=(context, output_queue, workflow_id))
    worker_process.start()

    # Wait a bit for dependency worker to process
    time.sleep(2)

    # Job 2 should now be READY since job 1 is completed
    assert context.job_registry.get_state(job_id2) == JobState.READY

    # Clean up
    context.job_registry.set_state(job_id2, workflow_id, output_queue, JobState.COMPLETED)
    worker_process.join(timeout=3)
    if worker_process.is_alive():
        worker_process.terminate()


def test_dependency_worker_waits_for_unsatisfied_dependency():
    """Test that dependency worker keeps jobs PENDING when dependencies are not yet satisfied."""

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    job_registry = SQLiteJobRegistry(tmp_file)
    job_registry.init("test-worker-5")

    context = MemoryContext(
        scope=LocalScope(jobs=[SimpleJob, JobWithOutput], dependencies=[JobDependency]), job_registry=job_registry
    )
    output_queue = multiprocessing.Queue()
    workflow_id = "test-workflow-5"

    # Add first job that is still running
    job1 = SimpleJob({"test": "data"}, {})
    job_id1 = context.job_registry.add(job1, output_queue)
    context.job_registry.set_state(job_id1, workflow_id, output_queue, JobState.RUNNING)

    # Add second job that depends on the first
    job_dependency = JobDependency(job_id1, context.job_registry)
    job2 = JobWithOutput({"count": 0}, {"job_dep": job_dependency})
    job_id2 = context.job_registry.add(job2, output_queue)

    # Job 2 should initially be PENDING
    assert context.job_registry.get_state(job_id2) == JobState.PENDING

    # Start dependency worker in a separate process
    worker_process = multiprocessing.Process(target=zahir_dependency_worker, args=(context, output_queue, workflow_id))
    worker_process.start()

    # Wait a bit for dependency worker to process
    time.sleep(2)

    # Job 2 should still be PENDING since job 1 is not completed
    assert context.job_registry.get_state(job_id2) == JobState.PENDING

    # Clean up - mark both as completed
    context.job_registry.set_state(job_id1, workflow_id, output_queue, JobState.COMPLETED)
    context.job_registry.set_state(job_id2, workflow_id, output_queue, JobState.COMPLETED)
    worker_process.join(timeout=3)
    if worker_process.is_alive():
        worker_process.terminate()


def test_dependency_worker_marks_impossible_on_failed_job_dependency():
    """Test that dependency worker marks jobs IMPOSSIBLE when a dependency job fails."""

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    job_registry = SQLiteJobRegistry(tmp_file)
    job_registry.init("test-worker-6")

    context = MemoryContext(
        scope=LocalScope(jobs=[SimpleJob, JobWithOutput], dependencies=[JobDependency]), job_registry=job_registry
    )
    output_queue = multiprocessing.Queue()
    workflow_id = "test-workflow-6"

    # Add first job that becomes irrecoverable
    job1 = SimpleJob({"test": "data"}, {})
    job_id1 = context.job_registry.add(job1, output_queue)
    context.job_registry.set_state(
        job_id1, workflow_id, output_queue, JobState.IRRECOVERABLE, error=Exception("Test failure")
    )

    # Add second job that depends on the first
    job_dependency = JobDependency(job_id1, context.job_registry)
    job2 = JobWithOutput({"count": 0}, {"job_dep": job_dependency})
    job_id2 = context.job_registry.add(job2, output_queue)

    # Job 2 should initially be PENDING
    assert context.job_registry.get_state(job_id2) == JobState.PENDING

    # Start dependency worker in a separate process
    worker_process = multiprocessing.Process(target=zahir_dependency_worker, args=(context, output_queue, workflow_id))
    worker_process.start()

    # Wait a bit for dependency worker to process
    time.sleep(2)

    # Job 2 should now be IMPOSSIBLE since job 1 is irrecoverable
    assert context.job_registry.get_state(job_id2) == JobState.IMPOSSIBLE

    # Worker should stop on its own
    worker_process.join(timeout=3)
    if worker_process.is_alive():
        worker_process.terminate()


def test_dependency_worker_handles_multiple_jobs():
    """Test that dependency worker can handle multiple jobs with different dependency states."""

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    job_registry = SQLiteJobRegistry(tmp_file)
    job_registry.init("test-worker-7")

    # Create satisfied and impossible dependencies
    past_time = datetime.datetime(2020, 1, 1, 0, 0, 0, tzinfo=datetime.UTC)
    satisfied_dep = TimeDependency(after=past_time)
    impossible_dep = TimeDependency(before=past_time)

    context = MemoryContext(
        scope=LocalScope(jobs=[SimpleJob], dependencies=[TimeDependency]), job_registry=job_registry
    )
    output_queue = multiprocessing.Queue()
    workflow_id = "test-workflow-7"

    # Add job with satisfied dependency
    job1 = SimpleJob({"id": 1}, {"time_dep": satisfied_dep})
    job_id1 = context.job_registry.add(job1, output_queue)

    # Add job with impossible dependency
    job2 = SimpleJob({"id": 2}, {"time_dep": impossible_dep})
    job_id2 = context.job_registry.add(job2, output_queue)

    # Both should initially be PENDING
    assert context.job_registry.get_state(job_id1) == JobState.PENDING
    assert context.job_registry.get_state(job_id2) == JobState.PENDING

    # Start dependency worker in a separate process
    worker_process = multiprocessing.Process(target=zahir_dependency_worker, args=(context, output_queue, workflow_id))
    worker_process.start()

    # Wait a bit for dependency worker to process
    time.sleep(2)

    # Job 1 should be READY, job 2 should be IMPOSSIBLE
    assert context.job_registry.get_state(job_id1) == JobState.READY
    assert context.job_registry.get_state(job_id2) == JobState.IMPOSSIBLE

    # Clean up
    context.job_registry.set_state(job_id1, workflow_id, output_queue, JobState.COMPLETED)
    worker_process.join(timeout=3)
    if worker_process.is_alive():
        worker_process.terminate()


def test_dependency_worker_only_processes_pending_jobs():
    """Test that dependency worker only processes jobs in PENDING state."""

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    job_registry = SQLiteJobRegistry(tmp_file)
    job_registry.init("test-worker-8")

    past_time = datetime.datetime(2020, 1, 1, 0, 0, 0, tzinfo=datetime.UTC)
    satisfied_dep = TimeDependency(after=past_time)

    context = MemoryContext(
        scope=LocalScope(jobs=[SimpleJob], dependencies=[TimeDependency]), job_registry=job_registry
    )
    output_queue = multiprocessing.Queue()
    workflow_id = "test-workflow-8"

    # Add a job in RUNNING state (not PENDING)
    job = SimpleJob({"test": "data"}, {"time_dep": satisfied_dep})
    job_id = context.job_registry.add(job, output_queue)
    context.job_registry.set_state(job_id, workflow_id, output_queue, JobState.RUNNING)

    # Start dependency worker in a separate process
    worker_process = multiprocessing.Process(target=zahir_dependency_worker, args=(context, output_queue, workflow_id))
    worker_process.start()

    # Wait a bit for dependency worker to process
    time.sleep(2)

    # Job should still be RUNNING (not changed to READY)
    assert context.job_registry.get_state(job_id) == JobState.RUNNING

    # Clean up
    context.job_registry.set_state(job_id, workflow_id, output_queue, JobState.COMPLETED)
    worker_process.join(timeout=3)
    if worker_process.is_alive():
        worker_process.terminate()


def test_dependency_worker_handles_internal_error():
    """Test that dependency worker emits ZahirInternalErrorEvent when an exception occurs."""

    # Create a mock job registry that raises an exception
    class BrokenJobRegistry:
        def init(self, worker_id: str) -> None:
            pass

        def is_active(self):
            raise RuntimeError("Simulated job registry failure")

    broken_registry = BrokenJobRegistry()

    context = MemoryContext(scope=LocalScope(jobs=[SimpleJob]), job_registry=broken_registry)
    output_queue = multiprocessing.Queue()
    workflow_id = "test-workflow-9"

    # Start dependency worker in a separate process
    worker_process = multiprocessing.Process(target=zahir_dependency_worker, args=(context, output_queue, workflow_id))
    worker_process.start()

    # Wait for worker to finish (should exit due to exception)
    worker_process.join(timeout=3)
    if worker_process.is_alive():
        worker_process.terminate()

    # Check that ZahirInternalErrorEvent was emitted
    event = output_queue.get(timeout=1)
    assert isinstance(event, ZahirInternalErrorEvent)
    assert event.workflow_id == workflow_id
    # error is a pickled blob, just check it exists and is not empty
    assert event.error
    assert len(event.error) > 0


def test_dependency_worker_direct_satisfied_path():
    """Direct unit test for dependency worker with satisfied dependency (no multiprocessing)."""

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    job_registry = SQLiteJobRegistry(tmp_file)
    job_registry.init("test-worker-direct-1")

    past_time = datetime.datetime(2020, 1, 1, 0, 0, 0, tzinfo=datetime.UTC)
    satisfied_dependency = TimeDependency(after=past_time)

    context = MemoryContext(
        scope=LocalScope(jobs=[SimpleJob], dependencies=[TimeDependency]), job_registry=job_registry
    )
    output_queue = multiprocessing.Queue()
    workflow_id = "test-workflow-direct-1"

    # Add a job with satisfied dependency
    job = SimpleJob({"test": "data"}, {"time_dep": satisfied_dependency})
    job_id = context.job_registry.add(job, output_queue)

    # Mock is_active to return True twice (to allow processing), then False
    call_count = [0]
    original_is_active = job_registry.is_active

    def mock_is_active():
        call_count[0] += 1
        if call_count[0] > 2:  # Allow two iterations
            return False
        return original_is_active()

    job_registry.is_active = mock_is_active

    # Run worker directly (not in subprocess) so coverage is tracked
    zahir_dependency_worker(context, output_queue, workflow_id)

    # Job should have been marked as READY
    assert context.job_registry.get_state(job_id) == JobState.READY

    # Drain any job state change events from the queue
    events = []
    while not output_queue.empty():
        events.append(output_queue.get(timeout=0.1))

    # WorkflowCompleteEvent should have been emitted (last event)
    assert len(events) > 0
    assert isinstance(events[-1], WorkflowCompleteEvent)


def test_dependency_worker_direct_impossible_path():
    """Direct unit test for dependency worker with impossible dependency (no multiprocessing)."""

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    job_registry = SQLiteJobRegistry(tmp_file)
    job_registry.init("test-worker-direct-2")

    past_time = datetime.datetime(2020, 1, 1, 0, 0, 0, tzinfo=datetime.UTC)
    impossible_dependency = TimeDependency(before=past_time)

    context = MemoryContext(
        scope=LocalScope(jobs=[SimpleJob], dependencies=[TimeDependency]), job_registry=job_registry
    )
    output_queue = multiprocessing.Queue()
    workflow_id = "test-workflow-direct-2"

    # Add a job with impossible dependency
    job = SimpleJob({"test": "data"}, {"time_dep": impossible_dependency})
    job_id = context.job_registry.add(job, output_queue)

    # Mock is_active to return True twice (to allow processing), then False
    call_count = [0]
    original_is_active = job_registry.is_active

    def mock_is_active():
        call_count[0] += 1
        if call_count[0] > 2:  # Allow two iterations
            return False
        return original_is_active()

    job_registry.is_active = mock_is_active

    # Run worker directly
    zahir_dependency_worker(context, output_queue, workflow_id)

    # Job should have been marked as IMPOSSIBLE
    assert context.job_registry.get_state(job_id) == JobState.IMPOSSIBLE

    # Drain any job state change events from the queue
    events = []
    while not output_queue.empty():
        events.append(output_queue.get(timeout=0.1))

    # WorkflowCompleteEvent should have been emitted (last event)
    assert len(events) > 0
    assert isinstance(events[-1], WorkflowCompleteEvent)


def test_dependency_worker_direct_no_jobs():
    """Direct unit test for dependency worker with no jobs."""

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    job_registry = SQLiteJobRegistry(tmp_file)
    job_registry.init("test-worker-direct-3")

    context = MemoryContext(scope=LocalScope(jobs=[SimpleJob]), job_registry=job_registry)
    output_queue = multiprocessing.Queue()
    workflow_id = "test-workflow-direct-3"

    # No jobs added - worker should exit immediately

    # Run worker directly
    zahir_dependency_worker(context, output_queue, workflow_id)

    # WorkflowCompleteEvent should have been emitted
    event = output_queue.get(timeout=1)
    assert isinstance(event, WorkflowCompleteEvent)
    assert event.workflow_id == workflow_id


def test_dependency_worker_direct_exception_handling():
    """Direct unit test for dependency worker exception handling."""

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    job_registry = SQLiteJobRegistry(tmp_file)
    job_registry.init("test-worker-direct-4")

    context = MemoryContext(scope=LocalScope(jobs=[SimpleJob]), job_registry=job_registry)
    output_queue = multiprocessing.Queue()
    workflow_id = "test-workflow-direct-4"

    # Make is_active raise an exception
    def mock_is_active_error():
        raise RuntimeError("Test exception in is_active")

    job_registry.is_active = mock_is_active_error

    # Run worker directly - should catch exception and emit error event
    zahir_dependency_worker(context, output_queue, workflow_id)

    # ZahirInternalErrorEvent should have been emitted
    event = output_queue.get(timeout=1)
    assert isinstance(event, ZahirInternalErrorEvent)
    assert event.workflow_id == workflow_id
    assert event.error
    assert len(event.error) > 0
