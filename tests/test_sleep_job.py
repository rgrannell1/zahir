"""Tests for the Sleep job.

Tests the Sleep job which pauses execution for a specified duration.
"""

import multiprocessing
import sys
import tempfile
import time

from zahir.base_types import Context, JobState
from zahir.context import MemoryContext
from zahir.events import Await, JobOutputEvent
from zahir.job_registry import SQLiteJobRegistry
from zahir.jobs import Empty
from zahir.jobs.decorator import spec
from zahir.jobs.sleep import Sleep
from zahir.scope import LocalScope
from zahir.worker import LocalWorkflow
from zahir.worker.call_frame import ZahirStackFrame
from zahir.worker.state_machine import ZahirWorkerState
from zahir.worker.state_machine.execute_job import execute_job
from zahir.worker.state_machine.states import HandleJobCompleteNoOutputStateChange


def test_sleep_job_precheck_valid_duration():
    """Test that Sleep job precheck accepts valid durations."""
    result = Sleep.precheck(None, {"duration_seconds": 1.5})
    assert result is None


def test_sleep_job_precheck_zero_duration():
    """Test that Sleep job precheck accepts zero duration."""
    result = Sleep.precheck(None, {"duration_seconds": 0})
    assert result is None


def test_sleep_job_precheck_invalid_duration_negative():
    """Test that Sleep job precheck rejects negative duration."""
    result = Sleep.precheck(None, {"duration_seconds": -1})
    assert isinstance(result, ValueError)
    assert "non-negative" in str(result)


def test_sleep_job_precheck_invalid_duration_type():
    """Test that Sleep job precheck rejects non-numeric duration."""
    result = Sleep.precheck(None, {"duration_seconds": "invalid"})
    assert isinstance(result, ValueError)


def test_sleep_job_precheck_missing_duration():
    """Test that Sleep job precheck accepts missing duration."""
    result = Sleep.precheck(None, {})
    assert result is None


def test_sleep_job_executes():
    """Test that Sleep job executes and yields events."""
    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    job_registry = SQLiteJobRegistry(tmp_file)
    job_registry.init("test-worker-sleep")

    context = MemoryContext(scope=LocalScope(), job_registry=job_registry)
    output_queue = multiprocessing.Queue()
    workflow_id = "test-workflow-sleep"

    # Test with a very short sleep to keep tests fast
    # Create JobInstance by calling the JobSpec
    job = Sleep({"duration_seconds": 0.1})
    job_id = context.job_registry.add(context, job, output_queue)

    worker_state = ZahirWorkerState(context, None, output_queue, workflow_id)
    job_generator = Sleep.run(None, context, job.input, job.dependencies)
    frame = ZahirStackFrame(job=job, job_generator=job_generator, recovery=False)
    worker_state.frame = frame

    # Execute the job - note that execute_job doesn't actually wait for dependencies
    # The timing happens in the dependency worker, not during job execution
    result, _ = execute_job(worker_state)

    # The job should have completed with output
    assert result is not None
    assert worker_state.last_event is not None


def test_sleep_job_stores_output():
    """Test that Sleep job yields output event with the duration."""
    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    job_registry = SQLiteJobRegistry(tmp_file)
    job_registry.init("test-worker-sleep-output")

    context = MemoryContext(scope=LocalScope(), job_registry=job_registry)
    output_queue = multiprocessing.Queue()
    workflow_id = "test-workflow-sleep-output"

    duration = 0.05
    # Create JobInstance by calling the JobSpec
    job = Sleep({"duration_seconds": duration})
    job_id = context.job_registry.add(context, job, output_queue)

    worker_state = ZahirWorkerState(context, None, output_queue, workflow_id)
    job_generator = Sleep.run(None, context, job.input, job.dependencies)
    frame = ZahirStackFrame(job=job, job_generator=job_generator, recovery=False)
    worker_state.frame = frame

    result, _ = execute_job(worker_state)

    # The last_event should contain the output event with the duration
    assert worker_state.last_event is not None
    assert hasattr(worker_state.last_event, "output")
    assert worker_state.last_event.output.get("duration_seconds") == duration


def test_sleep_job_very_small_duration():
    """Test that Sleep job works with very small durations."""
    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    job_registry = SQLiteJobRegistry(tmp_file)
    job_registry.init("test-worker-sleep-tiny")

    context = MemoryContext(scope=LocalScope(), job_registry=job_registry)
    output_queue = multiprocessing.Queue()
    workflow_id = "test-workflow-sleep-tiny"

    duration = 0.001
    job = Sleep({"duration_seconds": duration})
    job_id = context.job_registry.add(context, job, output_queue)

    worker_state = ZahirWorkerState(context, None, output_queue, workflow_id)
    job_generator = Sleep.run(None, context, job.input, job.dependencies)
    frame = ZahirStackFrame(job=job, job_generator=job_generator, recovery=False)
    worker_state.frame = frame

    result, _ = execute_job(worker_state)

    # Should complete successfully
    assert worker_state.last_event is not None
    assert worker_state.last_event.output.get("duration_seconds") == duration


def test_sleep_job_dependencies_structure():
    """Test that Sleep job sets up TimeDependency correctly."""
    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    job_registry = SQLiteJobRegistry(tmp_file)
    job_registry.init("test-worker-sleep-deps")

    context = MemoryContext(scope=LocalScope(), job_registry=job_registry)
    output_queue = multiprocessing.Queue()
    workflow_id = "test-workflow-sleep-deps"

    duration = 0.05
    job = Sleep({"duration_seconds": duration})
    job_generator = Sleep.run(None, context, job.input, job.dependencies)

    # Get the first yielded event (should be Empty with TimeDependency)
    first_event = next(job_generator)

    # Verify it's an Empty job with dependencies
    from zahir.jobs import Empty
    assert first_event.spec == Empty or first_event.__class__.__name__ == "JobInstance"

    # The dependency should have wait_seconds set
    assert first_event.dependencies.get("wait_seconds") is not None


def test_sleep_job_zero_duration():
    """Test that Sleep job executes immediately with zero duration."""
    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    job_registry = SQLiteJobRegistry(tmp_file)
    job_registry.init("test-worker-sleep-zero")

    context = MemoryContext(scope=LocalScope(), job_registry=job_registry)
    output_queue = multiprocessing.Queue()
    workflow_id = "test-workflow-sleep-zero"

    job = Sleep({"duration_seconds": 0})
    job_id = context.job_registry.add(context, job, output_queue)

    worker_state = ZahirWorkerState(context, None, output_queue, workflow_id)
    job_generator = Sleep.run(None, context, job.input, job.dependencies)
    frame = ZahirStackFrame(job=job, job_generator=job_generator, recovery=False)
    worker_state.frame = frame

    start_time = time.time()
    result, _ = execute_job(worker_state)
    elapsed_time = time.time() - start_time

    # Should complete very quickly (within reasonable time)
    assert elapsed_time < 1.0
    assert worker_state.last_event.output.get("duration_seconds") == 0


def test_sleep_job_float_duration():
    """Test that Sleep job handles float durations correctly."""
    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    job_registry = SQLiteJobRegistry(tmp_file)
    job_registry.init("test-worker-sleep-float")

    context = MemoryContext(scope=LocalScope(), job_registry=job_registry)
    output_queue = multiprocessing.Queue()
    workflow_id = "test-workflow-sleep-float"

    duration = 0.025
    job = Sleep({"duration_seconds": duration})
    job_id = context.job_registry.add(context, job, output_queue)

    worker_state = ZahirWorkerState(context, None, output_queue, workflow_id)
    job_generator = Sleep.run(None, context, job.input, job.dependencies)
    frame = ZahirStackFrame(job=job, job_generator=job_generator, recovery=False)
    worker_state.frame = frame

    result, _ = execute_job(worker_state)

    # Verify output contains exact duration
    assert worker_state.last_event.output.get("duration_seconds") == duration


@spec()
def SleepAndAwaitJob(spec_args, context: Context, input, dependencies):
    """A job that awaits Sleep."""
    sleep_result = yield Await(Sleep({"duration_seconds": 0.01}, {}))
    yield JobOutputEvent({"slept": True, "sleep_output": sleep_result})


def test_sleep_job_yields_correct_structure():
    """Test that Sleep job yields correct events with proper structure."""
    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    job_registry = SQLiteJobRegistry(tmp_file)
    job_registry.init("test-worker-sleep-structure")

    context = MemoryContext(scope=LocalScope(), job_registry=job_registry)

    duration = 0.05
    job = Sleep({"duration_seconds": duration})

    # Get the generator and collect all events
    job_generator = Sleep.run(None, context, job.input, job.dependencies)
    events = list(job_generator)

    # Should have 2 events: Empty (with dependency) and JobOutputEvent
    assert len(events) == 2

    empty_job = events[0]
    output_event = events[1]

    # First event should be an Empty job with wait_seconds dependency
    assert empty_job.spec == Empty or empty_job.__class__.__name__ == "JobInstance"
    # Check that wait_seconds is in the dependencies (DependencyGroup)
    assert hasattr(empty_job.dependencies, "dependencies")
    assert "wait_seconds" in empty_job.dependencies.dependencies

    # Second event should be JobOutputEvent with duration
    assert isinstance(output_event, JobOutputEvent)
    assert output_event.output.get("duration_seconds") == duration


def test_sleep_job_precheck_error_messages():
    """Test that precheck error messages are descriptive."""
    # Negative duration
    result = Sleep.precheck(None, {"duration_seconds": -5})
    assert isinstance(result, ValueError)
    assert "non-negative" in str(result).lower()

    # Non-numeric duration
    result = Sleep.precheck(None, {"duration_seconds": "not_a_number"})
    assert isinstance(result, ValueError)
    assert "number" in str(result).lower()

    # List instead of number
    result = Sleep.precheck(None, {"duration_seconds": [1, 2, 3]})
    assert isinstance(result, ValueError)

    # Dict instead of number
    result = Sleep.precheck(None, {"duration_seconds": {"value": 1}})
    assert isinstance(result, ValueError)


def test_sleep_job_large_duration():
    """Test precheck accepts large duration values."""
    result = Sleep.precheck(None, {"duration_seconds": 3600})
    assert result is None

    result = Sleep.precheck(None, {"duration_seconds": 999999.999})
    assert result is None


def test_sleep_job_output_event_type():
    """Test that Sleep job yields JobOutputEvent."""
    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    job_registry = SQLiteJobRegistry(tmp_file)
    job_registry.init("test-worker-sleep-output-type")

    context = MemoryContext(scope=LocalScope(), job_registry=job_registry)
    output_queue = multiprocessing.Queue()
    workflow_id = "test-workflow-sleep-output-type"

    duration = 0.02
    job = Sleep({"duration_seconds": duration})
    job_id = context.job_registry.add(context, job, output_queue)

    worker_state = ZahirWorkerState(context, None, output_queue, workflow_id)
    job_generator = Sleep.run(None, context, job.input, job.dependencies)

    # Iterate through job generator
    events = list(job_generator)

    # Should have at least 2 events: Empty (with dependency) and JobOutputEvent
    assert len(events) >= 2
    assert isinstance(events[-1], JobOutputEvent)
    assert events[-1].output.get("duration_seconds") == duration
