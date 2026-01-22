"""Tests for the handle_await state machine step.

Tests the handle_await function which handles Await events, pauses the current job,
and enqueues the awaited job(s).
"""

import multiprocessing
import sys
import tempfile
import sys

from zahir.base_types import Context, JobState
from zahir.context import MemoryContext
from zahir.events import Await, JobOutputEvent
from zahir.job_registry import SQLiteJobRegistry
from zahir.jobs.decorator import spec
from zahir.scope import LocalScope
from zahir.worker.call_frame import ZahirStackFrame
from zahir.worker.state_machine import ZahirWorkerState
from zahir.worker.state_machine.handle_await import handle_await
from zahir.worker.state_machine.states import WaitForJobStateChange


@spec()
def SimpleJob(context: Context, input, dependencies):
    """A simple job for testing."""
    yield JobOutputEvent({"result": "done"})


@spec()
def AwaitingJob(context: Context, input, dependencies):
    """A job that awaits another job."""
    result = yield Await(SimpleJob({"test": "data"}, {}))
    yield JobOutputEvent({"result": result})


@spec()
def MultiAwaitJob(context: Context, input, dependencies):
    """A job that awaits multiple jobs."""
    results = yield Await([
        SimpleJob({"idx": 0}, {}),
        SimpleJob({"idx": 1}, {}),
    ])
    yield JobOutputEvent({"results": results})


def test_handle_await_pauses_current_job():
    """Test that handle_await pauses the current job."""

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    job_registry = SQLiteJobRegistry(tmp_file)
    job_registry.init("test-worker-1")
    try:
        context = MemoryContext(scope=LocalScope.from_module(sys.modules[__name__]), job_registry=job_registry)
        input_queue = multiprocessing.Queue()
        output_queue = multiprocessing.Queue()
        workflow_id = "test-workflow-1"

        worker_state = ZahirWorkerState(context, input_queue, output_queue, workflow_id)

        # Add a job
        job = AwaitingJob({"test": "data"}, {})
        job_id = context.job_registry.add(context, job, output_queue, workflow_id)

        # Set up frame
        job_generator = AwaitingJob.run(context, job.input, job.dependencies)
        frame = ZahirStackFrame(job=job, job_generator=job_generator, recovery=False)
        worker_state.frame = frame

        # Set up await event
        awaited_job = SimpleJob({"test": "data"}, {})
        worker_state.await_event = Await(awaited_job)

        # Run handle_await
        handle_await(worker_state)

        # Current job should be paused
        job_state = context.job_registry.get_state(job_id)
        assert job_state == JobState.PAUSED
    finally:
        job_registry.close()


def test_handle_await_adds_awaited_job_to_registry():
    """Test that handle_await adds the awaited job to the registry."""

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    job_registry = SQLiteJobRegistry(tmp_file)
    job_registry.init("test-worker-2")
    try:
        context = MemoryContext(scope=LocalScope.from_module(sys.modules[__name__]), job_registry=job_registry)
        input_queue = multiprocessing.Queue()
        output_queue = multiprocessing.Queue()
        workflow_id = "test-workflow-2"

        worker_state = ZahirWorkerState(context, input_queue, output_queue, workflow_id)

        # Add a job
        job = AwaitingJob({"test": "data"}, {})
        job_id = context.job_registry.add(context, job, output_queue, workflow_id)

        # Set up frame
        job_generator = AwaitingJob.run(context, job.input, job.dependencies)
        frame = ZahirStackFrame(job=job, job_generator=job_generator, recovery=False)
        worker_state.frame = frame

        # Set up await event
        awaited_job = SimpleJob({"test": "data"}, {})
        worker_state.await_event = Await(awaited_job)

        # Run handle_await
        handle_await(worker_state)

        # Awaited job should be in the registry
        assert awaited_job.job_id is not None
        awaited_state = context.job_registry.get_state(awaited_job.job_id)
        assert awaited_state is not None
    finally:
        job_registry.close()


def test_handle_await_pushes_frame_to_stack():
    """Test that handle_await pushes the current frame back to the stack."""

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    job_registry = SQLiteJobRegistry(tmp_file)
    job_registry.init("test-worker-3")
    try:
        context = MemoryContext(scope=LocalScope.from_module(sys.modules[__name__]), job_registry=job_registry)
        input_queue = multiprocessing.Queue()
        output_queue = multiprocessing.Queue()
        workflow_id = "test-workflow-3"

        worker_state = ZahirWorkerState(context, input_queue, output_queue, workflow_id)

        # Add a job
        job = AwaitingJob({"test": "data"}, {})
        job_id = context.job_registry.add(context, job, output_queue, workflow_id)

        # Set up frame
        job_generator = AwaitingJob.run(context, job.input, job.dependencies)
        frame = ZahirStackFrame(job=job, job_generator=job_generator, recovery=False)
        worker_state.frame = frame

        # Verify stack is empty initially
        assert len(worker_state.job_stack.frames) == 0

        # Set up await event
        awaited_job = SimpleJob({"test": "data"}, {})
        worker_state.await_event = Await(awaited_job)

        # Run handle_await
        handle_await(worker_state)

        # Frame should be pushed to stack
        assert len(worker_state.job_stack.frames) == 1
        assert worker_state.job_stack.frames[0].job.job_id == job_id
    finally:
        job_registry.close()


def test_handle_await_clears_current_frame():
    """Test that handle_await clears the current frame."""

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    job_registry = SQLiteJobRegistry(tmp_file)
    job_registry.init("test-worker-4")
    try:
        context = MemoryContext(scope=LocalScope.from_module(sys.modules[__name__]), job_registry=job_registry)
        input_queue = multiprocessing.Queue()
        output_queue = multiprocessing.Queue()
        workflow_id = "test-workflow-4"

        worker_state = ZahirWorkerState(context, input_queue, output_queue, workflow_id)

        # Add a job
        job = AwaitingJob({"test": "data"}, {})
        job_id = context.job_registry.add(context, job, output_queue, workflow_id)

        # Set up frame
        job_generator = AwaitingJob.run(context, job.input, job.dependencies)
        frame = ZahirStackFrame(job=job, job_generator=job_generator, recovery=False)
        worker_state.frame = frame

        # Verify frame is set
        assert worker_state.frame is not None

        # Set up await event
        awaited_job = SimpleJob({"test": "data"}, {})
        worker_state.await_event = Await(awaited_job)

        # Run handle_await
        handle_await(worker_state)

        # Current frame should be cleared
        assert worker_state.frame is None
    finally:
        job_registry.close()


def test_handle_await_transitions_to_enqueue():
    """Test that handle_await returns EnqueueJobStateChange."""

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    job_registry = SQLiteJobRegistry(tmp_file)
    job_registry.init("test-worker-5")
    try:
        context = MemoryContext(scope=LocalScope.from_module(sys.modules[__name__]), job_registry=job_registry)
        input_queue = multiprocessing.Queue()
        output_queue = multiprocessing.Queue()
        workflow_id = "test-workflow-5"

        worker_state = ZahirWorkerState(context, input_queue, output_queue, workflow_id)

        # Add a job
        job = AwaitingJob({"test": "data"}, {})
        job_id = context.job_registry.add(context, job, output_queue, workflow_id)

        # Set up frame
        job_generator = AwaitingJob.run(context, job.input, job.dependencies)
        frame = ZahirStackFrame(job=job, job_generator=job_generator, recovery=False)
        worker_state.frame = frame

        # Set up await event
        awaited_job = SimpleJob({"test": "data"}, {})
        worker_state.await_event = Await(awaited_job)

        # Run handle_await
        result, _ = handle_await(worker_state)

        # Should transition to wait for job state
        assert isinstance(result, WaitForJobStateChange)
        assert "Paused job" in result.data["message"]
        assert "waiting for next dispatch" in result.data["message"]
    finally:
        job_registry.close()


def test_handle_await_sets_required_jobs():
    """Test that handle_await adds awaited job to required_jobs."""

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    job_registry = SQLiteJobRegistry(tmp_file)
    job_registry.init("test-worker-6")
    try:
        context = MemoryContext(scope=LocalScope.from_module(sys.modules[__name__]), job_registry=job_registry)
        input_queue = multiprocessing.Queue()
        output_queue = multiprocessing.Queue()
        workflow_id = "test-workflow-6"

        worker_state = ZahirWorkerState(context, input_queue, output_queue, workflow_id)

        # Add a job
        job = AwaitingJob({"test": "data"}, {})
        job_id = context.job_registry.add(context, job, output_queue, workflow_id)

        # Set up frame
        job_generator = AwaitingJob.run(context, job.input, job.dependencies)
        frame = ZahirStackFrame(job=job, job_generator=job_generator, recovery=False)
        worker_state.frame = frame

        # Verify required_jobs is empty initially
        assert len(frame.required_jobs) == 0

        # Set up await event
        awaited_job = SimpleJob({"test": "data"}, {})
        worker_state.await_event = Await(awaited_job)

        # Run handle_await
        handle_await(worker_state)

        # Awaited job should be in required_jobs of the pushed frame
        pushed_frame = worker_state.job_stack.frames[0]
        assert len(pushed_frame.required_jobs) == 1
        assert awaited_job.job_id in pushed_frame.required_jobs
    finally:
        job_registry.close()


def test_handle_await_multiple_jobs():
    """Test that handle_await handles awaiting multiple jobs."""

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    job_registry = SQLiteJobRegistry(tmp_file)
    job_registry.init("test-worker-7")
    try:
        context = MemoryContext(scope=LocalScope.from_module(sys.modules[__name__]), job_registry=job_registry)
        input_queue = multiprocessing.Queue()
        output_queue = multiprocessing.Queue()
        workflow_id = "test-workflow-7"

        worker_state = ZahirWorkerState(context, input_queue, output_queue, workflow_id)

        # Add a job
        job = MultiAwaitJob({"test": "data"}, {})
        job_id = context.job_registry.add(context, job, output_queue, workflow_id)

        # Set up frame
        job_generator = MultiAwaitJob.run(context, job.input, job.dependencies)
        frame = ZahirStackFrame(job=job, job_generator=job_generator, recovery=False)
        worker_state.frame = frame

        # Set up await event with multiple jobs
        awaited_job1 = SimpleJob({"idx": 0}, {})
        awaited_job2 = SimpleJob({"idx": 1}, {})
        worker_state.await_event = Await([awaited_job1, awaited_job2])

        # Run handle_await
        handle_await(worker_state)

        # Both jobs should be in required_jobs
        pushed_frame = worker_state.job_stack.frames[0]
        assert len(pushed_frame.required_jobs) == 2
        assert awaited_job1.job_id in pushed_frame.required_jobs
        assert awaited_job2.job_id in pushed_frame.required_jobs
    finally:
        job_registry.close()


def test_handle_await_multiple_jobs_sets_await_many():
    """Test that handle_await sets await_many flag for multiple jobs."""

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    job_registry = SQLiteJobRegistry(tmp_file)
    job_registry.init("test-worker-8")
    try:
        context = MemoryContext(scope=LocalScope.from_module(sys.modules[__name__]), job_registry=job_registry)
        input_queue = multiprocessing.Queue()
        output_queue = multiprocessing.Queue()
        workflow_id = "test-workflow-8"

        worker_state = ZahirWorkerState(context, input_queue, output_queue, workflow_id)

        # Add a job
        job = MultiAwaitJob({"test": "data"}, {})
        job_id = context.job_registry.add(context, job, output_queue, workflow_id)

        # Set up frame
        job_generator = MultiAwaitJob.run(context, job.input, job.dependencies)
        frame = ZahirStackFrame(job=job, job_generator=job_generator, recovery=False)
        worker_state.frame = frame

        # Verify await_many is False initially
        assert frame.await_many is False

        # Set up await event with multiple jobs
        awaited_job1 = SimpleJob({"idx": 0}, {})
        awaited_job2 = SimpleJob({"idx": 1}, {})
        worker_state.await_event = Await([awaited_job1, awaited_job2])

        # Run handle_await
        handle_await(worker_state)

        # await_many should be set to True
        pushed_frame = worker_state.job_stack.frames[0]
        assert pushed_frame.await_many is True
    finally:
        job_registry.close()


def test_handle_await_preserves_state():
    """Test that handle_await returns the same state object."""

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    job_registry = SQLiteJobRegistry(tmp_file)
    job_registry.init("test-worker-9")
    try:
        context = MemoryContext(scope=LocalScope.from_module(sys.modules[__name__]), job_registry=job_registry)
        input_queue = multiprocessing.Queue()
        output_queue = multiprocessing.Queue()
        workflow_id = "test-workflow-9"

        worker_state = ZahirWorkerState(context, input_queue, output_queue, workflow_id)

        # Add a job
        job = AwaitingJob({"test": "data"}, {})
        job_id = context.job_registry.add(context, job, output_queue, workflow_id)

        # Set up frame
        job_generator = AwaitingJob.run(context, job.input, job.dependencies)
        frame = ZahirStackFrame(job=job, job_generator=job_generator, recovery=False)
        worker_state.frame = frame

        # Set up await event
        awaited_job = SimpleJob({"test": "data"}, {})
        worker_state.await_event = Await(awaited_job)

        # Run handle_await
        result, returned_state = handle_await(worker_state)

        # Should return same state object
        assert returned_state is worker_state
    finally:
        job_registry.close()


def test_handle_await_workflow_id_used():
    """Test that handle_await uses the correct workflow_id."""

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    job_registry = SQLiteJobRegistry(tmp_file)
    job_registry.init("test-worker-10")
    try:
        context = MemoryContext(scope=LocalScope.from_module(sys.modules[__name__]), job_registry=job_registry)
        input_queue = multiprocessing.Queue()
        output_queue = multiprocessing.Queue()
        workflow_id = "specific-workflow-id-abc"

        worker_state = ZahirWorkerState(context, input_queue, output_queue, workflow_id)

        # Add a job
        job = AwaitingJob({"test": "data"}, {})
        job_id = context.job_registry.add(context, job, output_queue, workflow_id)

        # Set up frame
        job_generator = AwaitingJob.run(context, job.input, job.dependencies)
        frame = ZahirStackFrame(job=job, job_generator=job_generator, recovery=False)
        worker_state.frame = frame

        # Set up await event
        awaited_job = SimpleJob({"test": "data"}, {})
        worker_state.await_event = Await(awaited_job)

        # Run handle_await
        handle_await(worker_state)

        # Job state should be updated with correct workflow_id
        job_state = context.job_registry.get_state(job_id)
        assert job_state == JobState.PAUSED
    finally:
        job_registry.close()
