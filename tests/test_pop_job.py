"""Tests for the pop_job state machine step.

Tests the pop_job function which pops a runnable job from the stack
and determines the next state based on the job's state and timing.
"""

import multiprocessing
import tempfile
import time

from zahir.base_types import Context, JobState
from zahir.context import MemoryContext
from zahir.events import JobOutputEvent
from zahir.job_registry import SQLiteJobRegistry
from zahir.jobs.decorator import job
from zahir.scope import LocalScope
from zahir.worker.call_frame import ZahirStackFrame
from zahir.worker.state_machine import ZahirWorkerState
from zahir.worker.state_machine.pop_job import pop_job
from zahir.worker.state_machine.states import (
    CheckPreconditionsStateChange,
    ExecuteJobStateChange,
    HandleJobTimeoutStateChange,
    HandleRecoveryJobTimeoutStateChange,
)


@job()
def SimpleJob(context: Context, input, dependencies):
    """A simple job for testing."""
    yield JobOutputEvent({"result": "done"})


@job()
def AnotherJob(context: Context, input, dependencies):
    """Another job for testing."""
    yield JobOutputEvent({"count": 1})


def test_pop_job_ready_state_checks_preconditions():
    """Test that popping a READY job transitions to CheckPreconditionsStateChange."""

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    job_registry = SQLiteJobRegistry(tmp_file)
    job_registry.init("test-worker-1")

    context = MemoryContext(scope=LocalScope(jobs=[SimpleJob]), job_registry=job_registry)
    output_queue = multiprocessing.Queue()
    workflow_id = "test-workflow-1"

    worker_state = ZahirWorkerState(context, None, output_queue, workflow_id)

    # Add a job in READY state
    job = SimpleJob({"test": "data"}, {})
    job_id = context.job_registry.add(context, job, output_queue)

    # Push job to stack
    job_generator = SimpleJob.run(context, job.input, job.dependencies)
    frame = ZahirStackFrame(job=job, job_generator=job_generator, recovery=False)
    worker_state.job_stack.push(frame)

    # Run pop_job
    result, _ = pop_job(worker_state)

    # Should check preconditions for READY job
    assert isinstance(result, CheckPreconditionsStateChange)
    assert "SimpleJob" in result.data["message"]
    assert "claimed and active" in result.data["message"]

    # Frame should be set
    assert worker_state.frame is not None
    assert worker_state.frame.job.job_id == job_id


def test_pop_job_paused_state_executes():
    """Test that popping a PAUSED job transitions to ExecuteJobStateChange."""

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    job_registry = SQLiteJobRegistry(tmp_file)
    job_registry.init("test-worker-2")

    context = MemoryContext(scope=LocalScope(jobs=[SimpleJob]), job_registry=job_registry)
    output_queue = multiprocessing.Queue()
    workflow_id = "test-workflow-2"

    worker_state = ZahirWorkerState(context, None, output_queue, workflow_id)

    # Add a job and set it to PAUSED
    job = SimpleJob({"test": "data"}, {})
    job_id = context.job_registry.add(context, job, output_queue)
    context.job_registry.set_state(job.job_id, workflow_id, output_queue, JobState.PAUSED)

    # Add the child job that the parent is awaiting, and mark it as COMPLETED
    child_job = SimpleJob({"child": "data"}, {}, job_id="child-job-1")
    context.job_registry.add(context, child_job, output_queue)
    context.job_registry.set_state("child-job-1", workflow_id, output_queue, JobState.COMPLETED)

    # Push job to stack - with required_jobs set to simulate awaiting a child job
    job_generator = SimpleJob.run(context, job.input, job.dependencies)
    frame = ZahirStackFrame(job=job, job_generator=job_generator, recovery=False, required_jobs={"child-job-1"})
    worker_state.job_stack.push(frame)

    # Run pop_job
    result, _ = pop_job(worker_state)

    # Should execute the paused job
    assert isinstance(result, ExecuteJobStateChange)
    assert "Resuming job" in result.data["message"]
    assert "SimpleJob" in result.data["message"]


def test_pop_job_running_state_executes():
    """Test that popping a RUNNING job (resumed from await) transitions to ExecuteJobStateChange."""

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    job_registry = SQLiteJobRegistry(tmp_file)
    job_registry.init("test-worker-3")

    context = MemoryContext(scope=LocalScope(jobs=[SimpleJob]), job_registry=job_registry)
    output_queue = multiprocessing.Queue()
    workflow_id = "test-workflow-3"

    worker_state = ZahirWorkerState(context, None, output_queue, workflow_id)

    # Add a job and set it to RUNNING
    job = SimpleJob({"test": "data"}, {})
    job_id = context.job_registry.add(context, job, output_queue)
    context.job_registry.set_state(job.job_id, workflow_id, output_queue, JobState.RUNNING)

    # Add the child job that the parent is awaiting, and mark it as COMPLETED
    child_job = SimpleJob({"child": "data"}, {}, job_id="child-job-1")
    context.job_registry.add(context, child_job, output_queue)
    context.job_registry.set_state("child-job-1", workflow_id, output_queue, JobState.COMPLETED)

    # Push job to stack - with required_jobs set to simulate resuming from await
    job_generator = SimpleJob.run(context, job.input, job.dependencies)
    frame = ZahirStackFrame(job=job, job_generator=job_generator, recovery=False, required_jobs={"child-job-1"})
    worker_state.job_stack.push(frame)

    # Run pop_job
    result, _ = pop_job(worker_state)

    # Should execute the running job (resumed from await)
    assert isinstance(result, ExecuteJobStateChange)
    assert "Resuming job" in result.data["message"]
    # The job state is stored as RUNNING in registry
    assert "running" in result.data["message"].lower()


def test_pop_job_timeout_normal_job():
    """Test that a timed-out normal job transitions to HandleJobTimeoutStateChange."""

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    job_registry = SQLiteJobRegistry(tmp_file)
    job_registry.init("test-worker-4")

    context = MemoryContext(scope=LocalScope(jobs=[SimpleJob]), job_registry=job_registry)
    output_queue = multiprocessing.Queue()
    workflow_id = "test-workflow-4"

    worker_state = ZahirWorkerState(context, None, output_queue, workflow_id)

    # Add a job with timeout
    job = SimpleJob({"test": "data"}, {})
    from zahir.base_types import JobOptions

    job.job_options = JobOptions()
    job.job_options.job_timeout = 0.001

    job_id = context.job_registry.add(context, job, output_queue)
    context.job_registry.set_state(job.job_id, workflow_id, output_queue, JobState.RUNNING)

    # Sleep to ensure timeout
    time.sleep(0.01)

    # Push job to stack
    job_generator = SimpleJob.run(context, job.input, job.dependencies)
    frame = ZahirStackFrame(job=job, job_generator=job_generator, recovery=False)
    worker_state.job_stack.push(frame)

    # Run pop_job
    result, _ = pop_job(worker_state)

    # Should handle timeout
    assert isinstance(result, HandleJobTimeoutStateChange)
    assert "timed out" in result.data["message"].lower()


def test_pop_job_removes_from_stack():
    """Test that pop_job removes the job from the stack."""

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    job_registry = SQLiteJobRegistry(tmp_file)
    job_registry.init("test-worker-6")

    context = MemoryContext(scope=LocalScope(jobs=[SimpleJob]), job_registry=job_registry)
    output_queue = multiprocessing.Queue()
    workflow_id = "test-workflow-6"

    worker_state = ZahirWorkerState(context, None, output_queue, workflow_id)

    # Add a job
    job = SimpleJob({"test": "data"}, {})
    job_id = context.job_registry.add(context, job, output_queue)

    # Push job to stack
    job_generator = SimpleJob.run(context, job.input, job.dependencies)
    frame = ZahirStackFrame(job=job, job_generator=job_generator, recovery=False)
    worker_state.job_stack.push(frame)

    # Verify stack has one item
    assert len(worker_state.job_stack.frames) == 1

    # Run pop_job
    pop_job(worker_state)

    # Stack should now be empty
    assert len(worker_state.job_stack.frames) == 0


def test_pop_job_sets_active_frame():
    """Test that pop_job sets the popped job as the active frame."""

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    job_registry = SQLiteJobRegistry(tmp_file)
    job_registry.init("test-worker-7")

    context = MemoryContext(scope=LocalScope(jobs=[SimpleJob]), job_registry=job_registry)
    output_queue = multiprocessing.Queue()
    workflow_id = "test-workflow-7"

    worker_state = ZahirWorkerState(context, None, output_queue, workflow_id)

    # Add a job
    job = SimpleJob({"test": "data"}, {})
    job_id = context.job_registry.add(context, job, output_queue)

    # Push job to stack
    job_generator = SimpleJob.run(context, job.input, job.dependencies)
    frame = ZahirStackFrame(job=job, job_generator=job_generator, recovery=False)
    worker_state.job_stack.push(frame)

    # Verify no active frame initially
    assert worker_state.frame is None

    # Run pop_job
    pop_job(worker_state)

    # Frame should now be set
    assert worker_state.frame is not None
    assert worker_state.frame.job.job_id == job_id


def test_pop_job_multiple_jobs_pops_runnable():
    """Test that pop_job pops the correct runnable job when multiple are on stack."""

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    job_registry = SQLiteJobRegistry(tmp_file)
    job_registry.init("test-worker-8")

    context = MemoryContext(scope=LocalScope(jobs=[SimpleJob, AnotherJob]), job_registry=job_registry)
    output_queue = multiprocessing.Queue()
    workflow_id = "test-workflow-8"

    worker_state = ZahirWorkerState(context, None, output_queue, workflow_id)

    # Add first job (not runnable - waiting on something)
    job1 = SimpleJob({"order": 1}, {})
    job_id1 = context.job_registry.add(context, job1, output_queue)
    context.job_registry.set_state(job1.job_id, workflow_id, output_queue, JobState.PAUSED)

    # Add a dependency that's not finished
    dep_job = AnotherJob({"dep": True}, {})
    dep_id = context.job_registry.add(context, dep_job, output_queue)
    context.job_registry.set_state(dep_id, workflow_id, output_queue, JobState.RUNNING)

    # Create frame that requires the dependency
    job_generator1 = SimpleJob.run(context, job1.input, job1.dependencies)
    frame1 = ZahirStackFrame(job=job1, job_generator=job_generator1, recovery=False)
    frame1.required_jobs.add(dep_id)
    worker_state.job_stack.push(frame1)

    # Add second job (runnable)
    job2 = AnotherJob({"order": 2}, {})
    job_id2 = context.job_registry.add(context, job2, output_queue)

    # Push second job (runnable)
    job_generator2 = AnotherJob.run(context, job2.input, job2.dependencies)
    frame2 = ZahirStackFrame(job=job2, job_generator=job_generator2, recovery=False)
    worker_state.job_stack.push(frame2)

    # Run pop_job - should pop the runnable job (job2)
    result, _ = pop_job(worker_state)

    # Should have popped job2
    assert worker_state.frame.job.job_id == job_id2


def test_pop_job_preserves_state():
    """Test that pop_job returns the same state object."""

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    job_registry = SQLiteJobRegistry(tmp_file)
    job_registry.init("test-worker-9")

    context = MemoryContext(scope=LocalScope(jobs=[SimpleJob]), job_registry=job_registry)
    output_queue = multiprocessing.Queue()
    workflow_id = "test-workflow-9"

    worker_state = ZahirWorkerState(context, None, output_queue, workflow_id)

    # Add a job
    job = SimpleJob({"test": "data"}, {})
    job_id = context.job_registry.add(context, job, output_queue)

    # Push job to stack
    job_generator = SimpleJob.run(context, job.input, job.dependencies)
    frame = ZahirStackFrame(job=job, job_generator=job_generator, recovery=False)
    worker_state.job_stack.push(frame)

    # Run pop_job
    result, returned_state = pop_job(worker_state)

    # Should return same state object
    assert returned_state is worker_state


def test_pop_job_no_timeout_configured():
    """Test that jobs without timeout configuration proceed normally."""

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    job_registry = SQLiteJobRegistry(tmp_file)
    job_registry.init("test-worker-10")

    context = MemoryContext(scope=LocalScope(jobs=[SimpleJob]), job_registry=job_registry)
    output_queue = multiprocessing.Queue()
    workflow_id = "test-workflow-10"

    worker_state = ZahirWorkerState(context, None, output_queue, workflow_id)

    # Add a job with no timeout configured
    job = SimpleJob({"test": "data"}, {})
    job.job_options = None

    job_id = context.job_registry.add(context, job, output_queue)

    # Push job to stack
    job_generator = SimpleJob.run(context, job.input, job.dependencies)
    frame = ZahirStackFrame(job=job, job_generator=job_generator, recovery=False)
    worker_state.job_stack.push(frame)

    # Run pop_job
    result, _ = pop_job(worker_state)

    # Should not timeout, should check preconditions
    assert isinstance(result, CheckPreconditionsStateChange)


def test_pop_job_timeout_recovery_job():
    """Test that a timed-out recovery job transitions to HandleRecoveryJobTimeoutStateChange."""

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    job_registry = SQLiteJobRegistry(tmp_file)
    job_registry.init("test-worker-11")

    context = MemoryContext(scope=LocalScope(jobs=[SimpleJob]), job_registry=job_registry)
    output_queue = multiprocessing.Queue()
    workflow_id = "test-workflow-11"

    worker_state = ZahirWorkerState(context, None, output_queue, workflow_id)

    # Add a job with recovery timeout
    job = SimpleJob({"test": "data"}, {})
    from zahir.base_types import JobOptions

    job.job_options = JobOptions()
    job.job_options.recover_timeout = 0.001

    job_id = context.job_registry.add(context, job, output_queue)
    context.job_registry.set_state(job.job_id, workflow_id, output_queue, JobState.RECOVERING)

    # Sleep to ensure timeout
    time.sleep(0.01)

    # Push recovery job to stack
    job_generator = SimpleJob.run(context, job.input, job.dependencies)
    frame = ZahirStackFrame(job=job, job_generator=job_generator, recovery=True)
    worker_state.job_stack.push(frame)

    # Run pop_job
    result, _ = pop_job(worker_state)

    # Should handle recovery timeout

    assert isinstance(result, HandleRecoveryJobTimeoutStateChange)
    assert "timed out" in result.data["message"].lower()
    assert "recovery" in result.data["message"].lower()
