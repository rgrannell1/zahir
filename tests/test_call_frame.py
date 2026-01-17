"""Test ZahirCallStack and ZahirStackFrame classes."""

from collections.abc import Generator
from typing import Any
from unittest.mock import MagicMock

import pytest

from zahir.base_types import JobRegistry, JobState, JobInstance, JobArguments, JobSpec
from zahir.jobs.decorator import spec
from zahir.worker.call_frame import ZahirCallStack, ZahirStackFrame


@spec()
def MockJobSpec(spec_args, context, input, dependencies) -> Generator[Any, Any, Any]:
    """A mock job spec for testing."""
    yield "test"


def create_mock_job_instance(job_id: str = "test-job-1") -> JobInstance:
    """Create a mock JobInstance for testing."""
    job_args = JobArguments(
        job_id=job_id,
        parent_id=None,
        args={},
        dependencies={},
        job_timeout=None,
        recover_timeout=None,
    )
    return JobInstance(spec=MockJobSpec, args=job_args)


def create_mock_generator() -> Generator[Any, Any, Any]:
    """Create a simple mock generator for testing."""
    yield "value"


def create_mock_frame(
    job_id: str = "test-job-1",
    recovery: bool = False,
    required_jobs: set[str] | None = None,
    await_many: bool = False,
) -> ZahirStackFrame:
    """Helper to create a mock stack frame."""
    job = create_mock_job_instance(job_id)
    generator = create_mock_generator()

    return ZahirStackFrame(
        job=job,
        job_generator=generator,
        recovery=recovery,
        required_jobs=required_jobs or set(),
        await_many=await_many,
    )


# ================================================================
# ZahirStackFrame Tests
# ================================================================


def test_stack_frame_creation():
    """Test that a ZahirStackFrame can be created with required fields."""
    job = create_mock_job_instance()
    generator = create_mock_generator()

    frame = ZahirStackFrame(job=job, job_generator=generator)

    assert frame.job == job
    assert frame.job_generator == generator
    assert frame.recovery is False
    assert frame.required_jobs == set()
    assert frame.await_many is False


def test_stack_frame_creation_with_optional_fields():
    """Test creating a ZahirStackFrame with all optional fields set."""
    job = create_mock_job_instance()
    generator = create_mock_generator()
    required = {"job-1", "job-2"}

    frame = ZahirStackFrame(
        job=job,
        job_generator=generator,
        recovery=True,
        required_jobs=required,
        await_many=True,
    )

    assert frame.recovery is True
    assert frame.required_jobs == required
    assert frame.await_many is True


def test_stack_frame_job_type():
    """Test that job_type() returns the correct spec type."""
    frame = create_mock_frame()

    assert frame.job_type() == "MockJobSpec"


def test_stack_frame_required_jobs_mutability():
    """Test that required_jobs can be modified after creation."""
    frame = create_mock_frame()

    assert len(frame.required_jobs) == 0

    frame.required_jobs.add("job-1")
    frame.required_jobs.add("job-2")

    assert len(frame.required_jobs) == 2
    assert "job-1" in frame.required_jobs
    assert "job-2" in frame.required_jobs


# ================================================================
# ZahirCallStack Tests
# ================================================================


def test_call_stack_creation():
    """Test that a ZahirCallStack can be created."""
    stack = ZahirCallStack(frames=[])

    assert stack.frames == []
    assert stack.is_empty() is True


def test_call_stack_push():
    """Test pushing frames onto the call stack."""
    stack = ZahirCallStack(frames=[])
    frame1 = create_mock_frame(job_id="job-1")
    frame2 = create_mock_frame(job_id="job-2")

    stack.push(frame1)
    assert len(stack.frames) == 1
    assert stack.frames[0] == frame1

    stack.push(frame2)
    assert len(stack.frames) == 2
    assert stack.frames[1] == frame2


def test_call_stack_pop_from_top():
    """Test popping a frame from the top of the stack (default behavior)."""
    stack = ZahirCallStack(frames=[])
    frame1 = create_mock_frame(job_id="job-1")
    frame2 = create_mock_frame(job_id="job-2")

    stack.push(frame1)
    stack.push(frame2)

    popped = stack.pop()

    assert popped == frame2
    assert len(stack.frames) == 1
    assert stack.frames[0] == frame1


def test_call_stack_pop_from_index():
    """Test popping a frame from a specific index."""
    stack = ZahirCallStack(frames=[])
    frame1 = create_mock_frame(job_id="job-1")
    frame2 = create_mock_frame(job_id="job-2")
    frame3 = create_mock_frame(job_id="job-3")

    stack.push(frame1)
    stack.push(frame2)
    stack.push(frame3)

    popped = stack.pop(index=1)

    assert popped == frame2
    assert len(stack.frames) == 2
    assert stack.frames[0] == frame1
    assert stack.frames[1] == frame3


def test_call_stack_pop_empty_raises_error():
    """Test that popping from an empty stack raises IndexError."""
    stack = ZahirCallStack(frames=[])

    with pytest.raises(IndexError, match="pop from empty call stack"):
        stack.pop()


def test_call_stack_is_empty():
    """Test the is_empty method."""
    stack = ZahirCallStack(frames=[])

    assert stack.is_empty() is True

    stack.push(create_mock_frame())
    assert stack.is_empty() is False

    stack.pop()
    assert stack.is_empty() is True


def test_call_stack_top():
    """Test the top method returns the top frame without removing it."""
    stack = ZahirCallStack(frames=[])

    assert stack.top() is None

    frame1 = create_mock_frame(job_id="job-1")
    frame2 = create_mock_frame(job_id="job-2")

    stack.push(frame1)
    assert stack.top() == frame1
    assert len(stack.frames) == 1

    stack.push(frame2)
    assert stack.top() == frame2
    assert len(stack.frames) == 2


def test_call_stack_runnable_frame_idx_no_required_jobs():
    """Test runnable_frame_idx when frames have no required jobs."""
    stack = ZahirCallStack(frames=[])
    frame = create_mock_frame(job_id="job-1", required_jobs=set())
    stack.push(frame)

    # Create mock job registry
    job_registry = MagicMock(spec=JobRegistry)
    job_registry.get_state.return_value = JobState.PENDING

    idx = stack.runnable_frame_idx(job_registry)

    assert idx == 0


def test_call_stack_runnable_frame_idx_all_required_complete():
    """Test runnable_frame_idx when all required jobs are complete."""
    stack = ZahirCallStack(frames=[])
    frame = create_mock_frame(job_id="job-1", required_jobs={"dep-1", "dep-2"})
    stack.push(frame)

    # Create mock job registry that reports dependencies as finished
    job_registry = MagicMock(spec=JobRegistry)
    job_registry.get_state.return_value = JobState.PAUSED
    job_registry.is_finished.return_value = True

    idx = stack.runnable_frame_idx(job_registry)

    assert idx == 0
    assert job_registry.is_finished.call_count == 2


def test_call_stack_runnable_frame_idx_required_not_complete():
    """Test runnable_frame_idx when required jobs are not complete."""
    stack = ZahirCallStack(frames=[])
    frame = create_mock_frame(job_id="job-1", required_jobs={"dep-1", "dep-2"})
    stack.push(frame)

    # Create mock job registry where only one dependency is finished
    job_registry = MagicMock(spec=JobRegistry)
    job_registry.get_state.return_value = JobState.PAUSED
    job_registry.is_finished.side_effect = [True, False]

    idx = stack.runnable_frame_idx(job_registry)

    assert idx is None


def test_call_stack_runnable_frame_idx_multiple_frames():
    """Test runnable_frame_idx with multiple frames, returns bottom-most runnable."""
    stack = ZahirCallStack(frames=[])

    # Bottom frame with no dependencies (should be runnable)
    frame1 = create_mock_frame(job_id="job-1", required_jobs=set())
    # Top frame with dependencies (blocks unless complete)
    frame2 = create_mock_frame(job_id="job-2", required_jobs={"dep-1"})

    stack.push(frame1)
    stack.push(frame2)

    job_registry = MagicMock(spec=JobRegistry)
    job_registry.get_state.return_value = JobState.PENDING
    job_registry.is_finished.return_value = True

    # Should return index 1 (top frame) since its dependencies are complete
    idx = stack.runnable_frame_idx(job_registry)

    assert idx == 1


def test_call_stack_runnable_frame_idx_reversed_order():
    """Test that runnable_frame_idx processes frames in FIFO order (bottom to top)."""
    stack = ZahirCallStack(frames=[])

    # Create three frames
    frame1 = create_mock_frame(job_id="job-1", required_jobs={"dep-1"})
    frame2 = create_mock_frame(job_id="job-2", required_jobs={"dep-2"})
    frame3 = create_mock_frame(job_id="job-3", required_jobs=set())

    stack.push(frame1)
    stack.push(frame2)
    stack.push(frame3)

    job_registry = MagicMock(spec=JobRegistry)
    job_registry.get_state.return_value = JobState.PENDING

    # Third frame (top, index 2) has no dependencies so should be runnable
    idx = stack.runnable_frame_idx(job_registry)

    assert idx == 2


def test_call_stack_integration():
    """Integration test for typical stack operations."""
    stack = ZahirCallStack(frames=[])

    # Build up a stack
    for idx in range(3):
        frame = create_mock_frame(job_id=f"job-{idx}")
        stack.push(frame)

    assert len(stack.frames) == 3
    assert not stack.is_empty()
    assert stack.top().job.job_id == "job-2"

    # Pop frames one by one
    popped = stack.pop()
    assert popped.job.job_id == "job-2"
    assert len(stack.frames) == 2

    popped = stack.pop()
    assert popped.job.job_id == "job-1"
    assert len(stack.frames) == 1

    popped = stack.pop()
    assert popped.job.job_id == "job-0"
    assert stack.is_empty()
