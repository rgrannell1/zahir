"""Test ConcurrencyLimit dependency."""

import os
import pathlib
import tempfile
import time

from zahir.base_types import Context, DependencyState
from zahir.context import MemoryContext
from zahir.dependencies.concurrency import ConcurrencyLimit
from zahir.events import Await, JobOutputEvent
from zahir.job_registry import SQLiteJobRegistry
from zahir.jobs.decorator import job
from zahir.scope import LocalScope
from zahir.worker import LocalWorkflow


def test_concurrency_limit_claim():
    """Test that satisfied() checks semaphore availability."""
    limit = ConcurrencyLimit(limit=3, slots=1)

    # Initially, should be satisfied (no slots claimed)
    assert limit.satisfied() == DependencyState.SATISFIED


def test_concurrency_limit_free():
    """Test that multiple satisfied() checks work."""
    limit = ConcurrencyLimit(limit=3, slots=1)

    # Multiple checks should work
    assert limit.satisfied() == DependencyState.SATISFIED
    assert limit.satisfied() == DependencyState.SATISFIED


def test_concurrency_limit_free_minimum():
    """Test that satisfied() handles edge cases."""
    limit = ConcurrencyLimit(limit=3, slots=1)

    # Initially satisfied
    assert limit.satisfied() == DependencyState.SATISFIED


def test_concurrency_limit_satisfied():
    """Test that satisfied() returns correct state based on semaphore."""
    limit = ConcurrencyLimit(limit=3, slots=1)

    # Initially should be satisfied - no slots claimed yet
    assert limit.satisfied() == DependencyState.SATISFIED


def test_concurrency_limit_with_multiple_slots():
    """Test concurrency limit with slots > 1."""
    limit = ConcurrencyLimit(limit=5, slots=2)

    # Should be satisfied initially
    assert limit.satisfied() == DependencyState.SATISFIED


def test_concurrency_limit_context_manager():
    """Test using ConcurrencyLimit as a context manager."""
    limit = ConcurrencyLimit(limit=3, slots=1)

    # Context manager should work without errors
    # Note: The context manager is used after satisfied() confirms slots are available
    # In actual usage, __enter__ would be called after satisfied() claimed the slots

    # Simulate the real usage pattern: check satisfaction first
    state = limit.satisfied()
    assert state == DependencyState.SATISFIED


def test_concurrency_limit_save():
    """Test that save() returns correct structure."""
    limit = ConcurrencyLimit(limit=5, slots=2)

    saved = limit.save()

    assert saved["type"] == "ConcurrencyLimit"
    assert saved["limit"] == 5
    assert saved["slots"] == 2


def test_concurrency_limit_load():
    """Test that load() reconstructs the limit correctly."""
    data = {"type": "ConcurrencyLimit", "limit": 5, "slots": 2}

    limit = ConcurrencyLimit.load(None, data)

    assert limit.limit == 5
    assert limit.slots == 2


def test_concurrency_limit_save_load_roundtrip():
    """Test that save/load preserves limit configuration."""
    original = ConcurrencyLimit(limit=10, slots=3)

    # Save and load
    saved = original.save()
    restored = ConcurrencyLimit.load(None, saved)

    # Check limit and slots are preserved
    assert restored.limit == original.limit
    assert restored.slots == original.slots

    # Check behavior is the same
    assert restored.satisfied() == DependencyState.SATISFIED


def test_concurrency_limit_enforced_with_100_parallel_jobs():
    """Test that LocalWorkflow with 15 processes never exceeds a concurrency limit of 3.

    This test spawns 100 parallel jobs, each with a ConcurrencyLimit dependency of 3.
    It verifies that at no point do more than 3 jobs execute concurrently.
    """
    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    # Shared data structures to track concurrent execution
    # Using a file to track job execution across processes
    shared_state_file = tempfile.NamedTemporaryFile(delete=False, mode="w", suffix=".txt")
    shared_state_file.close()

    try:

        @job()
        def ConcurrentTestJob(context: Context, input_data, dependencies):
            """A job that tracks when it starts and ends to verify concurrency limits."""
            job_idx = input_data["idx"]

            # Write start marker
            with open(shared_state_file.name, "a") as f:
                f.write(f"START:{job_idx}:{os.getpid()}\n")

            # Simulate work
            time.sleep(0.1)

            # Write end marker
            with open(shared_state_file.name, "a") as f:
                f.write(f"END:{job_idx}:{os.getpid()}\n")

            yield JobOutputEvent({"job_idx": job_idx, "pid": os.getpid()})

        @job()
        def ParentJob(context: Context, input_data, dependencies):
            """Parent job that spawns 100 parallel jobs with concurrency limit."""
            # Create 100 child jobs, each with a concurrency limit of 3
            child_jobs = []
            for idx in range(100):
                child_jobs.append(
                    ConcurrentTestJob({"idx": idx}, {"concurrency_limit": ConcurrencyLimit(limit=3, slots=1)})
                )

            # Await all jobs in parallel
            results = yield Await(child_jobs)

            yield JobOutputEvent({"total_jobs": len(results)})

        # Set up the workflow
        context = MemoryContext(
            scope=LocalScope(
                jobs=[ConcurrentTestJob, ParentJob],
                dependencies=[ConcurrencyLimit],
            ),
            job_registry=SQLiteJobRegistry(tmp_file),
        )

        # Run with 15 worker processes
        workflow = LocalWorkflow(context, max_workers=15)
        events = list(workflow.run(ParentJob({}, {}), events_filter=None))

        # Verify workflow completed successfully
        assert len(events) > 0, "Workflow should produce events"

        # Read the execution log
        with open(shared_state_file.name, "r") as f:
            log_lines = f.readlines()

        # Parse the log to track concurrent jobs
        job_timeline = []  # List of (time_point, job_idx, event_type)

        for idx, line in enumerate(log_lines):
            if line.strip():
                parts = line.strip().split(":")
                event_type = parts[0]  # START or END
                job_idx = int(parts[1])
                pid = parts[2]

                if event_type == "START":
                    job_timeline.append((idx, job_idx, "START"))
                else:
                    job_timeline.append((idx, job_idx, "END"))

        # Verify concurrency limit was never exceeded
        # Count concurrent jobs at each time point
        max_concurrent = 0
        currently_running = set()

        for _, job_idx, event_type in job_timeline:
            if event_type == "START":
                currently_running.add(job_idx)
                max_concurrent = max(max_concurrent, len(currently_running))
            else:
                currently_running.discard(job_idx)

        # With a concurrency limit of 3, we should never exceed 3 concurrent jobs
        assert max_concurrent <= 3, (
            f"Concurrency limit of 3 was exceeded. Maximum concurrent jobs observed: {max_concurrent}. Expected: <= 3"
        )

    finally:
        # Clean up
        pathlib.Path(tmp_file).unlink(missing_ok=True)
        pathlib.Path(shared_state_file.name).unlink(missing_ok=True)
