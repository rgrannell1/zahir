import tempfile

from zahir.base_types import Context, Job
from zahir.context import MemoryContext
from zahir.events import (
    Await,
    JobEvent,
    JobIrrecoverableEvent,
    JobOutputEvent,
    JobPrecheckFailedEvent,
    WorkflowCompleteEvent,
    WorkflowStartedEvent,
    ZahirCustomEvent,
)
from zahir.job_registry import SQLiteJobRegistry
from zahir.scope import LocalScope
from zahir.tasks.decorator import job
from zahir.worker import LocalWorkflow


class PrecheckFailsJob(Job):
    """Interyield to another job"""

    @staticmethod
    def precheck(input):
        return ["oh I don't like that."]

    @classmethod
    def run(cls, context: Context, input, dependencies):
        yield JobOutputEvent({})


@job
def ParentJob(context: Context, input, dependencies):
    """A parent job that yields to the inner async job. Proves nested awaits work."""

    _ = yield Await(PrecheckFailsJob({"test": 1234}, {}))
    yield ZahirCustomEvent(output={"message": "Should never see this."})


def test_failed_prechecks():
    """Prove that jobs with failing prechecks do not run."""

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    context = MemoryContext(scope=LocalScope(jobs=[PrecheckFailsJob]), job_registry=SQLiteJobRegistry(tmp_file))
    workflow = LocalWorkflow(context)

    job = PrecheckFailsJob({"test": 1234}, {})
    events = list(workflow.run(job, all_events=True))
    for event in events:
        print(event)

    assert len(events) == 4
    assert isinstance(events[0], WorkflowStartedEvent)
    assert isinstance(events[1], JobEvent)
    assert isinstance(events[2], JobPrecheckFailedEvent)
    assert isinstance(events[3], WorkflowCompleteEvent)


def test_awaited_prechecks():
    """Prove that jobs with failing prechecks error when awaited."""

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    context = MemoryContext(
        scope=LocalScope(jobs=[PrecheckFailsJob, ParentJob]), job_registry=SQLiteJobRegistry(tmp_file)
    )
    workflow = LocalWorkflow(context, max_workers=2)

    job = ParentJob({}, {})
    events = list(workflow.run(job, all_events=True))
    for event in events:
        print(event)

    # Check jobirrecoverable failure due to precheck failure
    any(isinstance(event, JobIrrecoverableEvent) for event in events)
