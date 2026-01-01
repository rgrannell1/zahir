import datetime
import pathlib

from zahir.base_types import Context
from zahir.context import MemoryContext
from zahir.dependencies.time import TimeDependency
from zahir.events import (
    Await,
    JobCompletedEvent,
    JobEvent,
    JobIrrecoverableEvent,
    JobOutputEvent,
    JobPausedEvent,
    JobRecoveryStartedEvent,
    JobStartedEvent,
    WorkflowCompleteEvent,
    WorkflowOutputEvent,
    WorkflowStartedEvent,
    ZahirCustomEvent,
)
from zahir.job_registry import SQLiteJobRegistry
from zahir.scope import LocalScope
from zahir.tasks.decorator import job
from zahir.worker import LocalWorkflow


@job
def AddJob(context: Context, input, dependencies):
    """Add to the input count and yield it"""

    yield JobOutputEvent({"count": input["count"] + 1})


@job
def YieldMany(context: Context, input, dependencies):
    """Interyield to another job"""

    count = 0
    result = yield Await(AddJob({"count": count}, {}))
    count = result["count"]
    result = yield Await(AddJob({"count": count}, {}))
    count = result["count"]

    yield JobOutputEvent({"count": count})


@job
def ParentJob(context: Context, input, dependencies):
    """A parent job that yields to the inner async job. Proves nested awaits work."""

    subcount = yield Await(YieldMany({}, {}))
    yield WorkflowOutputEvent({"count": subcount["count"]})


def test_nested_async_workflow():
    """Prove that a workflow can use Await many times"""

    tmp_file = "/tmp/zahir_yield_many.db"
    pathlib.Path(tmp_file).unlink() if pathlib.Path(tmp_file).exists() else None
    pathlib.Path(tmp_file).touch(exist_ok=True)

    context = MemoryContext(
        scope=LocalScope(jobs=[AddJob, YieldMany, ParentJob]), job_registry=SQLiteJobRegistry(tmp_file)
    )

    workflow = LocalWorkflow(context)

    yield_many = ParentJob({}, {})
    events = list(workflow.run(yield_many))
    assert len(events) == 1
    final_event = events[0]
    assert isinstance(final_event, WorkflowOutputEvent)
    assert final_event.output["count"] == 2


@job
def ImpossibleParentJob(context: Context, input, dependencies):
    """A parent job that yields to an impossible inner job."""

    dependency = TimeDependency(before=datetime.datetime(2000, 1, 1, 0, 0, 0, tzinfo=datetime.UTC))

    yield Await(AddJob({"count": -1}, {"impossible_dependency": dependency}))
    yield ZahirCustomEvent(output={"This should never be reached"})


def test_impossible_async_workflow():
    """Prove that an impossible job in an async workflow is handled correctly"""

    tmp_file = "/tmp/zahir_yield_many_impossible.db"
    pathlib.Path(tmp_file).unlink() if pathlib.Path(tmp_file).exists() else None
    pathlib.Path(tmp_file).touch(exist_ok=True)

    scope = LocalScope(
        jobs=[AddJob, YieldMany, ImpossibleParentJob],
        dependencies=[TimeDependency],
    )
    context = MemoryContext(scope=scope, job_registry=SQLiteJobRegistry(tmp_file))

    workflow = LocalWorkflow(context)

    blocked = ImpossibleParentJob({}, {})
    events = list(workflow.run(blocked, all_events=True))

    assert isinstance(events[0], WorkflowStartedEvent)
    assert isinstance(events[1], JobEvent)
    assert isinstance(events[2], JobStartedEvent)
    assert isinstance(events[3], JobEvent)
    assert isinstance(events[4], JobPausedEvent)
    assert isinstance(events[5], JobStartedEvent)
    assert isinstance(events[6], JobRecoveryStartedEvent)
    assert isinstance(events[7], JobStartedEvent)
    assert isinstance(events[8], JobIrrecoverableEvent)
    assert isinstance(events[9], WorkflowCompleteEvent)

@job
def AwaitMany(context: Context, input, dependencies):
    """Interyield to multiple jobs"""

    results = yield Await(
        [
            AddJob({"count": 10}, {}),
            AddJob({"count": 20}, {}),
            AddJob({"count": 30}, {}),
        ]
    )

    total = sum(result["count"] for result in results)

    yield WorkflowOutputEvent({"total": total})

def test_await_many_workflow():
    """Prove that Await can handle multiple jobs"""

    tmp_file = "/tmp/zahir_await_many.db"
    pathlib.Path(tmp_file).unlink() if pathlib.Path(tmp_file).exists() else None
    pathlib.Path(tmp_file).touch(exist_ok=True)

    scope = LocalScope(
        jobs=[AddJob, AwaitMany],
        dependencies=[],
    )

    context = MemoryContext(scope=scope, job_registry=SQLiteJobRegistry(tmp_file))
    workflow = LocalWorkflow(context, max_workers=2)

    blocked = AwaitMany({}, {})
    events = list(workflow.run(blocked, all_events=True))

    assert isinstance(events[0], WorkflowStartedEvent)
    assert isinstance(events[1], JobEvent)
    assert isinstance(events[2], JobStartedEvent)
    assert isinstance(events[3], JobEvent)
    assert isinstance(events[4], JobEvent)
    assert isinstance(events[5], JobEvent)
    assert isinstance(events[6], JobPausedEvent)
    assert isinstance(events[7], JobStartedEvent)
    assert isinstance(events[8], JobOutputEvent)
    assert isinstance(events[9], JobCompletedEvent)
    assert isinstance(events[10], JobStartedEvent)
    assert isinstance(events[11], JobOutputEvent)
    assert isinstance(events[12], JobCompletedEvent)
    assert isinstance(events[13], JobStartedEvent)
    assert isinstance(events[14], JobOutputEvent)
    assert isinstance(events[15], JobCompletedEvent)
    assert isinstance(events[16], JobStartedEvent)
    assert isinstance(events[17], WorkflowOutputEvent)
    assert isinstance(events[18], JobCompletedEvent)
    assert isinstance(events[19], WorkflowCompleteEvent)

    workflow_output = events[17]
    assert workflow_output.output["total"] == 63

@job
def AwaitEmpty(context: Context, input, dependencies):
    """Interyield to an empty list of jobs"""

    results = yield Await([])

    yield WorkflowOutputEvent({"total": len(results)})

def test_await_empty_workflow():
    """Prove that Await can handle an empty list of jobs"""

    tmp_file = "/tmp/zahir_await_empty.db"
    pathlib.Path(tmp_file).unlink() if pathlib.Path(tmp_file).exists() else None
    pathlib.Path(tmp_file).touch(exist_ok=True)

    scope = LocalScope(
        jobs=[AwaitEmpty],
        dependencies=[],
    )

    context = MemoryContext(scope=scope, job_registry=SQLiteJobRegistry(tmp_file))
    workflow = LocalWorkflow(context)

    blocked = AwaitEmpty({}, {})
    events = list(workflow.run(blocked, all_events=True))

    assert isinstance(events[0], WorkflowStartedEvent)
    assert isinstance(events[1], JobEvent)
    assert isinstance(events[2], JobStartedEvent)
    assert isinstance(events[3], JobPausedEvent)
    assert isinstance(events[4], JobStartedEvent)
    assert isinstance(events[5], WorkflowOutputEvent)
    assert isinstance(events[6], JobCompletedEvent)
    assert isinstance(events[7], WorkflowCompleteEvent)

    workflow_output = events[5]
    assert workflow_output.output["total"] == 0


@job
def FailingJob(context: Context, input, dependencies):
    """A job that always fails."""

    raise ValueError("This job always fails.")
    yield iter([])

@job
def AwaitManyFailing(context: Context, input, dependencies):
    """Interyield to multiple jobs, one of which fails."""

    try:
        results = yield Await(
            [
                AddJob({"count": 10}, {}),
                FailingJob({}, {}),
                AddJob({"count": 30}, {}),
            ]
        )
    except ValueError as err:
        yield WorkflowOutputEvent({"error": str(err)})
        return

def test_await_many_failing_workflow():
    """Prove that Await can handle multiple jobs with one failing"""

    tmp_file = "/tmp/zahir_await_many_failing.db"
    pathlib.Path(tmp_file).unlink() if pathlib.Path(tmp_file).exists() else None
    pathlib.Path(tmp_file).touch(exist_ok=True)

    scope = LocalScope(
        jobs=[AddJob, FailingJob, AwaitManyFailing],
        dependencies=[],
    )

    context = MemoryContext(scope=scope, job_registry=SQLiteJobRegistry(tmp_file))
    workflow = LocalWorkflow(context, max_workers=2)

    blocked = AwaitManyFailing({}, {})
    events = list(workflow.run(blocked, all_events=True))

    raise Exception("this should produce an output, but does not!")
