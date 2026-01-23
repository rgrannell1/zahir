"""Tests for process_await utility function.

Tests the process_await function which converts dependencies to Empty jobs
for awaiting.
"""

import sys
import tempfile

from zahir.base_types import Context, JobInstance
from zahir.context import MemoryContext
from zahir.dependencies.group import DependencyGroup
from zahir.dependencies.job import JobDependency
from zahir.dependencies.time import TimeDependency
from zahir.events import Await, JobOutputEvent
from zahir.job_registry import SQLiteJobRegistry
from zahir.jobs import Empty
from zahir.jobs.decorator import spec
from zahir.scope import LocalScope
from zahir.worker.state_machine.utils import process_await
from datetime import datetime, timedelta, UTC


@spec()
def SimpleJob(context: Context, input, dependencies):
    """A simple job for testing."""
    yield JobOutputEvent({"result": "done"})


def test_process_await_single_job_instance():
    """Test that process_await returns single JobInstance unchanged."""

    job = SimpleJob({"test": "data"}, {})
    await_event = Await(job)

    result = process_await(await_event)

    assert isinstance(result.job, JobInstance)
    assert result.job.job_id == job.job_id
    assert result.job.spec.type == "SimpleJob"


def test_process_await_single_dependency():
    """Test that process_await wraps a single Dependency in an Empty job."""

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    job_registry = SQLiteJobRegistry(tmp_file)
    job_registry.init("test-worker")

    context = MemoryContext(
        scope=LocalScope.from_module(sys.modules[__name__]), job_registry=job_registry
    )

    # Create a dependency
    dependency = TimeDependency(
        after=datetime.now(UTC) - timedelta(seconds=1),
        before=datetime.now(UTC) + timedelta(seconds=10),
    )

    await_event = Await(dependency)
    result = process_await(await_event)

    # Should be wrapped in Empty job
    assert isinstance(result.job, JobInstance)
    assert result.job.spec.type == "Empty"
    # Check that the dependency is in the dependencies
    deps = result.job.dependencies.get("dependencies")
    # The dependency should be accessible (wrapped in DependencyGroup by JobInstance.__call__)
    assert deps is not None


def test_process_await_list_of_jobs():
    """Test that process_await returns list of JobInstances unchanged."""

    job1 = SimpleJob({"idx": 0}, {})
    job2 = SimpleJob({"idx": 1}, {})
    await_event = Await([job1, job2])

    result = process_await(await_event)

    assert isinstance(result.job, list)
    assert len(result.job) == 2
    assert all(isinstance(j, JobInstance) for j in result.job)
    assert result.job[0].job_id == job1.job_id
    assert result.job[1].job_id == job2.job_id


def test_process_await_list_of_dependencies():
    """Test that process_await wraps multiple dependencies in a single Empty job."""

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    job_registry = SQLiteJobRegistry(tmp_file)
    job_registry.init("test-worker")

    context = MemoryContext(
        scope=LocalScope.from_module(sys.modules[__name__]), job_registry=job_registry
    )

    # Create multiple dependencies
    dep1 = TimeDependency(
        after=datetime.now(UTC) - timedelta(seconds=1),
        before=datetime.now(UTC) + timedelta(seconds=10),
    )
    dep2 = TimeDependency(
        after=datetime.now(UTC) - timedelta(seconds=1),
        before=datetime.now(UTC) + timedelta(seconds=20),
    )

    await_event = Await([dep1, dep2])
    result = process_await(await_event)

    # Should be wrapped in Empty job with both dependencies
    assert isinstance(result.job, list)
    assert len(result.job) == 1
    assert isinstance(result.job[0], JobInstance)
    assert result.job[0].spec.type == "Empty"

    # Check dependencies are in the Empty job
    deps = result.job[0].dependencies.get("dependencies")
    # Should have dependencies (wrapped in DependencyGroup)
    assert deps is not None


def test_process_await_mixed_jobs_and_dependencies():
    """Test that process_await partitions jobs and dependencies correctly."""

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    job_registry = SQLiteJobRegistry(tmp_file)
    job_registry.init("test-worker")

    context = MemoryContext(
        scope=LocalScope.from_module(sys.modules[__name__]), job_registry=job_registry
    )

    # Create a mix of jobs and dependencies
    job1 = SimpleJob({"idx": 0}, {})
    job2 = SimpleJob({"idx": 1}, {})

    dep1 = TimeDependency(
        after=datetime.now(UTC) - timedelta(seconds=1),
        before=datetime.now(UTC) + timedelta(seconds=10),
    )
    dep2 = TimeDependency(
        after=datetime.now(UTC) - timedelta(seconds=1),
        before=datetime.now(UTC) + timedelta(seconds=20),
    )

    await_event = Await([job1, dep1, job2, dep2])
    result = process_await(await_event)

    # Should have jobs + one Empty job with dependencies
    assert isinstance(result.job, list)
    assert len(result.job) == 3  # 2 jobs + 1 Empty job

    # Check jobs are preserved
    job_ids = {j.job_id for j in result.job if j.spec.type == "SimpleJob"}
    assert job1.job_id in job_ids
    assert job2.job_id in job_ids

    # Check Empty job exists with dependencies
    empty_jobs = [j for j in result.job if j.spec.type == "Empty"]
    assert len(empty_jobs) == 1
    empty_job = empty_jobs[0]
    assert "dependencies" in empty_job.dependencies.dependencies


def test_process_await_single_dependency_group():
    """Test that process_await handles DependencyGroup correctly."""

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    job_registry = SQLiteJobRegistry(tmp_file)
    job_registry.init("test-worker")

    context = MemoryContext(
        scope=LocalScope.from_module(sys.modules[__name__]), job_registry=job_registry
    )

    # Create a DependencyGroup
    dep1 = TimeDependency(
        after=datetime.now(UTC) - timedelta(seconds=1),
        before=datetime.now(UTC) + timedelta(seconds=10),
    )
    dep2 = TimeDependency(
        after=datetime.now(UTC) - timedelta(seconds=1),
        before=datetime.now(UTC) + timedelta(seconds=20),
    )
    dependency_group = DependencyGroup({"time1": dep1, "time2": dep2})

    await_event = Await(dependency_group)
    result = process_await(await_event)

    # Should be wrapped in Empty job
    assert isinstance(result.job, JobInstance)
    assert result.job.spec.type == "Empty"
    deps = result.job.dependencies.get("dependencies")
    assert isinstance(deps, DependencyGroup)


def test_process_await_list_with_dependency_group():
    """Test that process_await handles DependencyGroup in a list."""

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    job_registry = SQLiteJobRegistry(tmp_file)
    job_registry.init("test-worker")

    context = MemoryContext(
        scope=LocalScope.from_module(sys.modules[__name__]), job_registry=job_registry
    )

    job = SimpleJob({"test": "data"}, {})

    dep1 = TimeDependency(
        after=datetime.now(UTC) - timedelta(seconds=1),
        before=datetime.now(UTC) + timedelta(seconds=10),
    )
    dependency_group = DependencyGroup({"time": dep1})

    await_event = Await([job, dependency_group])
    result = process_await(await_event)

    # Should have job + Empty job
    assert isinstance(result.job, list)
    assert len(result.job) == 2

    # One should be the original job
    job_ids = {j.job_id for j in result.job if j.spec.type == "SimpleJob"}
    assert job.job_id in job_ids

    # One should be Empty with the dependency
    empty_jobs = [j for j in result.job if j.spec.type == "Empty"]
    assert len(empty_jobs) == 1


def test_process_await_empty_list():
    """Test that process_await handles empty list."""

    await_event = Await([])
    result = process_await(await_event)

    assert isinstance(result.job, list)
    assert len(result.job) == 0


def test_process_await_list_only_dependencies_creates_empty_job():
    """Test that when list contains only dependencies, creates one Empty job."""

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    job_registry = SQLiteJobRegistry(tmp_file)
    job_registry.init("test-worker")

    context = MemoryContext(
        scope=LocalScope.from_module(sys.modules[__name__]), job_registry=job_registry
    )

    dep1 = TimeDependency(
        after=datetime.now(UTC) - timedelta(seconds=1),
        before=datetime.now(UTC) + timedelta(seconds=10),
    )
    dep2 = TimeDependency(
        after=datetime.now(UTC) - timedelta(seconds=1),
        before=datetime.now(UTC) + timedelta(seconds=20),
    )

    await_event = Await([dep1, dep2])
    result = process_await(await_event)

    # Should create one Empty job with both dependencies
    assert isinstance(result.job, list)
    assert len(result.job) == 1
    assert result.job[0].spec.type == "Empty"
    # Check dependencies are present
    deps = result.job[0].dependencies.get("dependencies")
    assert deps is not None


def test_process_await_preserves_job_order():
    """Test that process_await preserves order of jobs in mixed list."""

    job1 = SimpleJob({"idx": 0}, {})
    job2 = SimpleJob({"idx": 1}, {})

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    job_registry = SQLiteJobRegistry(tmp_file)
    job_registry.init("test-worker")

    context = MemoryContext(
        scope=LocalScope.from_module(sys.modules[__name__]), job_registry=job_registry
    )

    dep = TimeDependency(
        after=datetime.now(UTC) - timedelta(seconds=1),
        before=datetime.now(UTC) + timedelta(seconds=10),
    )

    # Jobs should come first, then Empty job
    await_event = Await([job1, dep, job2])
    result = process_await(await_event)

    assert isinstance(result.job, list)
    assert len(result.job) == 3
    # First two should be the original jobs
    assert result.job[0].job_id == job1.job_id
    assert result.job[1].job_id == job2.job_id
    # Last should be Empty
    assert result.job[2].spec.type == "Empty"


def test_process_await_invalid_item_raises():
    """Test that process_await raises ValueError for invalid items."""

    await_event = Await(["not a job or dependency"])  # type: ignore

    try:
        process_await(await_event)
        assert False, "Should have raised ValueError"
    except ValueError as err:
        assert "Invalid awaitable item" in str(err)


def test_process_await_invalid_awaitable_raises():
    """Test that process_await raises ValueError for invalid awaitable type."""

    await_event = Await("not a valid awaitable")  # type: ignore

    try:
        process_await(await_event)
        assert False, "Should have raised ValueError"
    except ValueError as err:
        assert "Invalid awaitable" in str(err)
