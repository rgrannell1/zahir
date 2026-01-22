import sys
import tempfile
import time

from zahir.base_types import Context
from zahir.context.memory import MemoryContext
from zahir.events import (
    Await,
    JobEvent,
    JobIrrecoverableEvent,
    JobOutputEvent,
    JobPausedEvent,
    JobRecoveryStartedEvent,
    JobRecoveryTimeoutEvent,
    JobStartedEvent,
    JobTimeoutEvent,
    WorkflowCompleteEvent,
    WorkflowStartedEvent,
    ZahirCustomEvent,
)
from zahir.exception import exception_from_text_blob
from zahir.job_registry.sqlite import SQLiteJobRegistry
from zahir.jobs.decorator import spec
from zahir.scope import LocalScope
from zahir.worker import LocalWorkflow


@spec()
def Adder(context, input, dependencies):
    time.sleep(5)
    yield JobOutputEvent({"count": input["count"] + 1})


@spec()
def TimeOutRunner(context, input, dependencies):
    yield Await(Adder({"count": 0}, {}, job_timeout=2))

    yield ZahirCustomEvent(output={"message": "this should never be seen"})


def test_timeout():
    """Prove a job can actually time out."""

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    context = MemoryContext(
        scope=LocalScope.from_module(sys.modules[__name__]), job_registry=SQLiteJobRegistry(tmp_file)
    )
    workflow = LocalWorkflow(context)

    job = TimeOutRunner({}, {})
    events = list(workflow.run(job, events_filter=None))

    # timeouts propegate into irrecoverable errors
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
    assert str(exception_from_text_blob(events[10].error)) == "Job execution timed out"
    assert isinstance(events[11], WorkflowCompleteEvent)


def _fails_then_recovers_recover(context, input, dependencies, err):
    """Recovery function that takes too long"""
    time.sleep(5)
    yield JobOutputEvent({"recovered": True})


@spec(recover=_fails_then_recovers_recover)
def FailsThenRecoversSlowly(context: Context, input, dependencies):
    raise Exception("Simulated Failure")
    yield iter([])


@spec()
def RecoveryTimeoutRunner(context, input, dependencies):
    yield Await(FailsThenRecoversSlowly({}, {}, recover_timeout=2))
    yield ZahirCustomEvent(output={"message": "this should never be seen"})


def test_recovery_timeout():
    """Prove a job recovery can actually time out."""

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    context = MemoryContext(
        scope=LocalScope.from_module(sys.modules[__name__]),
        job_registry=SQLiteJobRegistry(tmp_file),
    )
    workflow = LocalWorkflow(context)

    job = RecoveryTimeoutRunner({}, {})
    events = list(workflow.run(job, events_filter=None))

    assert isinstance(events[0], WorkflowStartedEvent)
    assert isinstance(events[1], JobEvent)
    assert isinstance(events[2], JobStartedEvent)
    assert isinstance(events[3], JobEvent)
    assert isinstance(events[4], JobPausedEvent)
    assert isinstance(events[5], JobStartedEvent)
    assert isinstance(events[6], JobRecoveryStartedEvent)
    assert isinstance(events[7], JobStartedEvent)
    assert isinstance(events[8], JobRecoveryTimeoutEvent)
    assert isinstance(events[9], JobStartedEvent)
    assert isinstance(events[10], JobRecoveryStartedEvent)
    assert isinstance(events[11], JobStartedEvent)
    assert isinstance(events[12], JobIrrecoverableEvent)
    assert str(exception_from_text_blob(events[12].error)) == "Recovery job execution timed out"
    assert isinstance(events[13], WorkflowCompleteEvent)
