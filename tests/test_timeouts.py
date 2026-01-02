
import tempfile
import time
from zahir.base_types import JobOptions
from zahir.context.memory import MemoryContext
from zahir.events import Await, JobEvent, JobIrrecoverableEvent, JobOutputEvent, JobPausedEvent, JobRecoveryStartedEvent, JobStartedEvent, JobTimeoutEvent, WorkflowCompleteEvent, WorkflowStartedEvent, ZahirCustomEvent
from zahir.job_registry.sqlite import SQLiteJobRegistry
from zahir.jobs.decorator import job
from zahir.scope import LocalScope
from zahir.worker import LocalWorkflow


@job
def Adder(context, input, dependencies):
  time.sleep(5)
  yield JobOutputEvent({
    "count": input["count"] + 1
  })

@job
def TimeOutRunner(context, input, dependencies):
  yield Await(Adder(
     input={ "count": 0 },
     dependencies={},
     options=JobOptions(job_timeout=2)))

  yield ZahirCustomEvent(output={ "message": "this should never be seen" })

def test_timeout():
    """Prove a job can actually time out."""

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    context = MemoryContext(scope=LocalScope(jobs=[TimeOutRunner, Adder]), job_registry=SQLiteJobRegistry(tmp_file))
    workflow = LocalWorkflow(context)

    job = TimeOutRunner({}, {})
    events = list(workflow.run(job, all_events=True))

    assert isinstance(events[0], WorkflowStartedEvent)
    assert isinstance(events[1], JobEvent)
    assert isinstance(events[2], JobStartedEvent)
    assert isinstance(events[3], JobEvent)
    assert isinstance(events[4], JobPausedEvent)
    assert isinstance(events[5], JobStartedEvent)
    assert isinstance(events[6], JobTimeoutEvent)
    assert isinstance(events[7], JobStartedEvent)
    assert isinstance(events[8], JobRecoveryStartedEvent)
    assert isinstance(events[9], JobStartedEvent)
    assert isinstance(events[10], JobIrrecoverableEvent)
    assert isinstance(events[11], WorkflowCompleteEvent)

test_timeout()
