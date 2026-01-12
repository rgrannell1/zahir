"""Test that many jobs distribute across multiple workers."""

import os
import tempfile
import time

import pytest

from zahir.base_types import Context
from zahir.context import MemoryContext
from zahir.events import Await, JobOutputEvent
from zahir.job_registry import SQLiteJobRegistry
from zahir.jobs.decorator import job
from zahir.scope import LocalScope
from zahir.worker import LocalWorkflow


@job()
def CPUBoundJob(context: Context, input, dependencies):
    """A trivial job that returns its process ID."""

    time.sleep(1)

    yield JobOutputEvent({"pid": os.getpid(), "idx": input["idx"]})


@job()
def SpawnManyJobs(context: Context, input, dependencies):
    """Spawns 50 CPU-bound jobs."""

    jobs = [CPUBoundJob({"idx": i}, {}) for i in range(50)]

    results = yield Await(jobs)
    yield JobOutputEvent({"count": len(results)})


@pytest.mark.timeout(10)
def test_many_jobs_distribute_across_workers():
    """Test that 50 jobs actually distribute across 3 available workers."""

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    context = MemoryContext(
        scope=LocalScope(jobs=[CPUBoundJob, SpawnManyJobs]),
        job_registry=SQLiteJobRegistry(tmp_file),
    )

    # With max_workers=4, we get: 1 dependency worker + 3 job workers
    workflow = LocalWorkflow(context, max_workers=15)
    events = list(workflow.run(SpawnManyJobs({}, {}), events_filter=None))

    # Debug: print events to see what's happening
    from zahir.events import JobIrrecoverableEvent

    for event in events:
        if isinstance(event, JobIrrecoverableEvent):
            print(f"Job failed: {event}")
            if hasattr(event, "exception"):
                print(f"Exception: {event.exception}")

    # Collect all PIDs from job outputs
    pids = []
    for event in events:
        if isinstance(event, JobOutputEvent) and "pid" in event.output:
            pids.append(event.output["pid"])

    # Verify we got 50 job outputs
    assert len(pids) == 50, f"Expected 50 CPU job outputs, got {len(pids)}"

    # Count unique processes
    unique_pids = set(pids)
    num_processes = len(unique_pids)

    print(f"\nJob distribution across workers:")
    for pid in sorted(unique_pids):
        count = pids.count(pid)
        print(f"  Worker {pid}: {count} jobs")

    # With 15 job workers available, we should use more than 10 process
    assert num_processes > 10, (
        f"Expected jobs to run on multiple processes (15 workers available), "
        f"but only used {num_processes} process(es). Distribution: {dict((pid, pids.count(pid)) for pid in unique_pids)}"
    )

    # Ideally should use all 3 workers, but at minimum 2
    assert num_processes >= 10, f"Expected at least 10 workers to be used, got {num_processes}"
