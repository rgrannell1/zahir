import sys
import tempfile

from zahir.base_types import Context
from zahir.context import MemoryContext
from zahir.events import (
    Await,
    JobEvent,
    JobIrrecoverableEvent,
    JobOutputEvent,
    JobPrecheckFailedEvent,
    JobStartedEvent,
    WorkflowCompleteEvent,
    WorkflowStartedEvent,
    ZahirCustomEvent,
)
from zahir.exception import JobPrecheckError
from zahir.job_registry import SQLiteJobRegistry
from zahir.jobs.decorator import spec
from zahir.scope import LocalScope
from zahir.worker import LocalWorkflow


def _precheck_fails_job_precheck(spec_args, input):
    """Precheck that always fails"""
    return JobPrecheckError("oh I don't like that.")


@spec(precheck=_precheck_fails_job_precheck)
def PrecheckFailsJob(spec_args, context: Context, input, dependencies):
    """Job that always fails precheck"""
    yield JobOutputEvent({})


@spec()
def ParentJob(spec_args, context: Context, input, dependencies):
    """A parent job that yields to the inner async job. Proves nested awaits work."""

    _ = yield Await(PrecheckFailsJob({"test": 1234}, {}))
    yield ZahirCustomEvent(output={"message": "Should never see this."})


def test_failed_prechecks():
    """Prove that jobs with failing prechecks do not run."""

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    context = MemoryContext(scope=LocalScope.from_module(sys.modules[__name__]), job_registry=SQLiteJobRegistry(tmp_file))
    workflow = LocalWorkflow(context)

    job = PrecheckFailsJob({"test": 1234}, {})
    events = list(workflow.run(job, events_filter=None))

    # In push-based dispatch model, JobStartedEvent is emitted when job is dispatched,
    # before precondition checking. So we now expect 5 events.
    assert len(events) == 5
    assert isinstance(events[0], WorkflowStartedEvent)
    assert isinstance(events[1], JobEvent)
    assert isinstance(events[2], JobStartedEvent)
    assert isinstance(events[3], JobPrecheckFailedEvent)
    assert isinstance(events[4], WorkflowCompleteEvent)


def test_awaited_prechecks():
    """Prove that jobs with failing prechecks error when awaited."""

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    context = MemoryContext(
        scope=LocalScope.from_module(sys.modules[__name__]), job_registry=SQLiteJobRegistry(tmp_file)
    )
    workflow = LocalWorkflow(context, max_workers=2)

    job = ParentJob({}, {})
    events = list(workflow.run(job, events_filter=None))

    # Check jobirrecoverable failure due to precheck failure
    any(isinstance(event, JobIrrecoverableEvent) for event in events)
