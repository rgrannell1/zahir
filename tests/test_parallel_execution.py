"""Test that jobs actually run in parallel on multiple cores."""

import multiprocessing
import os
import tempfile
import time

from zahir.base_types import Context
from zahir.context import MemoryContext
from zahir.events import JobCompletedEvent, JobOutputEvent
from zahir.job_registry import SQLiteJobRegistry
from zahir.jobs.decorator import spec
from zahir.scope import LocalScope
import sys
from zahir.worker import LocalWorkflow


@spec()
def CPUBoundJob(spec_args, context: Context, input, dependencies):
    """A CPU-bound job that tracks which process it runs on."""

    yield JobOutputEvent({"pid": os.getpid(), "job_idx": input["job_idx"]})


def test_jobs_run_on_multiple_processes():
    """Test that jobs actually run on different worker processes (cores)."""

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    # Create a parent job that spawns multiple CPU-bound jobs
    @spec()
    def ParentJob(spec_args, context: Context, input, dependencies):
        """Parent job that spawns multiple CPU-bound child jobs."""
        from zahir.events import Await

        # Spawn 4 CPU-bound jobs
        jobs = [CPUBoundJob({"job_idx": idx}, {}, 0.1) for idx in range(4)]
        yield Await(jobs)

    scope = LocalScope.from_module(sys.modules[__name__])
    scope.add_job_spec(ParentJob)
    context = MemoryContext(
        scope=scope,
        job_registry=SQLiteJobRegistry(tmp_file),
    )

    # With max_workers=4, we get: 1 dependency worker + 3 job workers
    workflow = LocalWorkflow(context, max_workers=4)
    events = list(workflow.run(ParentJob({}, {}, 0.1), events_filter=None))

    # Extract PIDs from CPUBoundJob outputs
    cpu_job_outputs = [e.output for e in events if isinstance(e, JobOutputEvent) and "pid" in e.output]
    assert len(cpu_job_outputs) == 4, f"Expected 4 CPU job outputs, got {len(cpu_job_outputs)}"

    pids = [output["pid"] for output in cpu_job_outputs]
    num_processes = len(set(pids))

    # With 4 jobs and 3 job workers, jobs should be distributed across multiple processes
    assert num_processes > 1, (
        f"Expected jobs to run on multiple processes (3 workers available), but only used {num_processes} process(es): {pids}"
    )
    assert all(pid > 0 for pid in pids), f"All PIDs should be positive: {pids}"


def test_max_workers_limits_parallelism():
    """Test that max_workers setting actually limits concurrent execution."""

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    # Create a parent job that spawns multiple CPU-bound jobs
    @spec()
    def ParentJob2(spec_args, context: Context, input, dependencies):
        """Parent job that spawns multiple CPU-bound child jobs."""
        from zahir.events import Await

        # Spawn 4 CPU-bound jobs
        jobs = [CPUBoundJob({"job_idx": idx}, {}, 0.1) for idx in range(4)]
        yield Await(jobs)

    scope = LocalScope.from_module(sys.modules[__name__])
    scope.add_job_spec(ParentJob2)
    context = MemoryContext(
        scope=scope,
        job_registry=SQLiteJobRegistry(tmp_file),
    )

    workflow = LocalWorkflow(context, max_workers=2)
    events = list(workflow.run(ParentJob2({}, {}, 0.1), events_filter=None))

    # Extract PIDs from CPUBoundJob outputs
    cpu_job_outputs = [e.output for e in events if isinstance(e, JobOutputEvent) and "pid" in e.output]
    assert len(cpu_job_outputs) == 4, f"Expected 4 CPU job outputs, got {len(cpu_job_outputs)}"

    pids = [output["pid"] for output in cpu_job_outputs]
    num_processes = len(set(pids))

    # With max_workers=2, we should see at most 2 different PIDs (1 for dependency worker, 1 for job worker)
    assert num_processes <= 2, f"With max_workers=2, expected at most 2 processes, got {num_processes}: {pids}"
