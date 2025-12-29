import pathlib
import tempfile

from zahir.base_types import Context
from zahir.context import MemoryContext
from zahir.events import (
    JobCompletedEvent,
    JobOutputEvent,
    JobStartedEvent,
    WorkflowCompleteEvent,
    WorkflowStartedEvent,
    ZahirCustomEvent,
)
from zahir.job_registry import SQLiteJobRegistry
from zahir.scope import LocalScope
from zahir.tasks.decorator import job
from zahir.worker import LocalWorkflow


@job
def JustReturns(cls, context: Context, input, dependencies):
    """Interyield to another job"""

    yield ZahirCustomEvent(output={"message": "This should be seen"})
    yield JobOutputEvent({})
    yield ZahirCustomEvent(output={"message": "This should never be seen"})


def test_nested_async_workflow():
    """Prove that no events are consumed after job output is returned. Also document the normal execution flow."""

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    context = MemoryContext(scope=LocalScope(jobs=[JustReturns]), job_registry=SQLiteJobRegistry(tmp_file))

    workflow = LocalWorkflow(context)

    job = JustReturns({}, {})
    events = list(workflow.run(job, all_events=True))
    print(events)

    assert len(events) == 6
    assert isinstance(events[0], WorkflowStartedEvent)
    assert isinstance(events[1], JobStartedEvent)
    assert isinstance(events[2], ZahirCustomEvent)
    assert events[2].output == {"message": "This should be seen"}

    assert isinstance(events[3], JobOutputEvent)
    assert isinstance(events[4], JobCompletedEvent)
    assert isinstance(events[5], WorkflowCompleteEvent)
