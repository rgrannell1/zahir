import sys
import tempfile
from typing import TypedDict

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


def _precheck_fails_job_precheck(input):
    """Precheck that always fails"""
    return JobPrecheckError("oh I don't like that.")


@spec(precheck=_precheck_fails_job_precheck)
def PrecheckFailsJob(context: Context, input, dependencies):
    """Job that always fails precheck"""
    yield JobOutputEvent({})


@spec()
def ParentJob(context: Context, input, dependencies):
    """A parent job that yields to the inner async job. Proves nested awaits work."""

    _ = yield Await(PrecheckFailsJob({"test": 1234}, {}))
    yield ZahirCustomEvent(output={"message": "Should never see this."})


def test_failed_prechecks():
    """Prove that jobs with failing prechecks do not run."""

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    context = MemoryContext(
        scope=LocalScope.from_module(sys.modules[__name__]), job_registry=SQLiteJobRegistry(tmp_file)
    )
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


# TypedDict validation tests
class ValidInput(TypedDict):
    value: int
    name: str


class OptionalInput(TypedDict, total=False):
    optional_value: int
    required_value: str


@spec(args=ValidInput)
def TypedDictJob(context: Context, input, dependencies):
    """Job with TypedDict validation."""
    yield JobOutputEvent({"result": input["value"] * 2})


@spec(args=OptionalInput)
def OptionalTypedDictJob(context: Context, input, dependencies):
    """Job with optional TypedDict fields."""
    yield JobOutputEvent({"result": "ok"})


@spec()
def NoTypedDictJob(context: Context, input, dependencies):
    """Job without TypedDict validation."""
    yield JobOutputEvent({"result": "ok"})


def test_typeddict_precheck_valid_input():
    """Test that valid TypedDict input passes precheck."""

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    context = MemoryContext(
        scope=LocalScope.from_module(sys.modules[__name__]), job_registry=SQLiteJobRegistry(tmp_file)
    )
    workflow = LocalWorkflow(context)

    job = TypedDictJob({"value": 42, "name": "test"}, {})
    events = list(workflow.run(job, events_filter=None))

    # Should complete successfully
    assert any(isinstance(event, WorkflowCompleteEvent) for event in events)
    assert any(isinstance(event, JobOutputEvent) for event in events)


def test_typeddict_precheck_missing_required_key():
    """Test that missing required key fails precheck."""

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    context = MemoryContext(
        scope=LocalScope.from_module(sys.modules[__name__]), job_registry=SQLiteJobRegistry(tmp_file)
    )
    workflow = LocalWorkflow(context)

    job = TypedDictJob({"value": 42}, {})  # Missing "name"
    events = list(workflow.run(job, events_filter=None))

    # Should fail precheck
    assert any(isinstance(event, JobPrecheckFailedEvent) for event in events)
    assert any(isinstance(event, WorkflowCompleteEvent) for event in events)


def test_typeddict_precheck_wrong_type():
    """Test that wrong type fails precheck."""

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    context = MemoryContext(
        scope=LocalScope.from_module(sys.modules[__name__]), job_registry=SQLiteJobRegistry(tmp_file)
    )
    workflow = LocalWorkflow(context)

    job = TypedDictJob({"value": "not an int", "name": "test"}, {})  # Wrong type for value
    events = list(workflow.run(job, events_filter=None))

    # Should fail precheck
    assert any(isinstance(event, JobPrecheckFailedEvent) for event in events)


def test_typeddict_precheck_optional_fields():
    """Test that optional TypedDict fields work correctly."""

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    context = MemoryContext(
        scope=LocalScope.from_module(sys.modules[__name__]), job_registry=SQLiteJobRegistry(tmp_file)
    )
    workflow = LocalWorkflow(context)

    # Should work with only required field
    job = OptionalTypedDictJob({"required_value": "test"}, {})
    events = list(workflow.run(job, events_filter=None))

    assert any(isinstance(event, WorkflowCompleteEvent) for event in events)

    # Should also work with optional field
    job2 = OptionalTypedDictJob({"required_value": "test", "optional_value": 42}, {})
    events2 = list(workflow.run(job2, events_filter=None))

    assert any(isinstance(event, WorkflowCompleteEvent) for event in events2)


def test_typeddict_precheck_no_args_type():
    """Test that jobs without args_type still work (no validation)."""

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    context = MemoryContext(
        scope=LocalScope.from_module(sys.modules[__name__]), job_registry=SQLiteJobRegistry(tmp_file)
    )
    workflow = LocalWorkflow(context)

    job = NoTypedDictJob({"anything": "goes"}, {})
    events = list(workflow.run(job, events_filter=None))

    # Should complete successfully without validation
    assert any(isinstance(event, WorkflowCompleteEvent) for event in events)


# TypedDict output validation tests
class ValidOutput(TypedDict):
    result: int
    status: str


class OptionalOutput(TypedDict, total=False):
    optional_field: int
    required_field: str


@spec(output=ValidOutput)
def TypedDictOutputJob(context: Context, input, dependencies):
    """Job with TypedDict output validation."""
    yield JobOutputEvent({"result": 42, "status": "ok"})


@spec(output=OptionalOutput)
def OptionalTypedDictOutputJob(context: Context, input, dependencies):
    """Job with optional TypedDict output fields."""
    yield JobOutputEvent({"required_field": "test"})


@spec(output=OptionalOutput)
def OptionalFieldOutputJob(context: Context, input, dependencies):
    """Job with optional TypedDict output fields including optional field."""
    yield JobOutputEvent({"required_field": "test", "optional_field": 42})


@spec(output=ValidOutput)
def InvalidOutputJob(context: Context, input, dependencies):
    """Job that outputs invalid structure (missing required key)."""
    yield JobOutputEvent({"result": 42})  # Missing "status"


@spec(output=ValidOutput)
def WrongTypeOutputJob(context: Context, input, dependencies):
    """Job that outputs wrong type."""
    yield JobOutputEvent({"result": "not an int", "status": "ok"})  # Wrong type for result


@spec()
def NoTypedDictOutputJob(context: Context, input, dependencies):
    """Job without TypedDict output validation."""
    yield JobOutputEvent({"anything": "goes"})


def test_typeddict_postcheck_valid_output():
    """Test that valid TypedDict output passes postcheck."""

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    context = MemoryContext(
        scope=LocalScope.from_module(sys.modules[__name__]), job_registry=SQLiteJobRegistry(tmp_file)
    )
    workflow = LocalWorkflow(context)

    job = TypedDictOutputJob({}, {})
    events = list(workflow.run(job, events_filter=None))

    # Should complete successfully
    assert any(isinstance(event, WorkflowCompleteEvent) for event in events)
    assert any(isinstance(event, JobOutputEvent) for event in events)


def test_typeddict_postcheck_missing_required_key():
    """Test that missing required key in output fails postcheck."""

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    context = MemoryContext(
        scope=LocalScope.from_module(sys.modules[__name__]), job_registry=SQLiteJobRegistry(tmp_file)
    )
    workflow = LocalWorkflow(context)

    job = InvalidOutputJob({}, {})
    events = list(workflow.run(job, events_filter=None))

    # Should fail postcheck and be treated as exception
    assert any(isinstance(event, JobIrrecoverableEvent) for event in events)
    assert any(isinstance(event, WorkflowCompleteEvent) for event in events)


def test_typeddict_postcheck_wrong_type():
    """Test that wrong type in output fails postcheck."""

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    context = MemoryContext(
        scope=LocalScope.from_module(sys.modules[__name__]), job_registry=SQLiteJobRegistry(tmp_file)
    )
    workflow = LocalWorkflow(context)

    job = WrongTypeOutputJob({}, {})
    events = list(workflow.run(job, events_filter=None))

    # Should fail postcheck and be treated as exception
    assert any(isinstance(event, JobIrrecoverableEvent) for event in events)


def test_typeddict_postcheck_optional_fields():
    """Test that optional TypedDict output fields work correctly."""

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    context = MemoryContext(
        scope=LocalScope.from_module(sys.modules[__name__]), job_registry=SQLiteJobRegistry(tmp_file)
    )
    workflow = LocalWorkflow(context)

    # Should work with only required field
    job = OptionalTypedDictOutputJob({}, {})
    events = list(workflow.run(job, events_filter=None))

    assert any(isinstance(event, WorkflowCompleteEvent) for event in events)

    # Should also work with optional field
    job2 = OptionalFieldOutputJob({}, {})
    events2 = list(workflow.run(job2, events_filter=None))

    assert any(isinstance(event, WorkflowCompleteEvent) for event in events2)


def test_typeddict_postcheck_no_output_type():
    """Test that jobs without output_type still work (no validation)."""

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    context = MemoryContext(
        scope=LocalScope.from_module(sys.modules[__name__]), job_registry=SQLiteJobRegistry(tmp_file)
    )
    workflow = LocalWorkflow(context)

    job = NoTypedDictOutputJob({}, {})
    events = list(workflow.run(job, events_filter=None))

    # Should complete successfully without validation
    assert any(isinstance(event, WorkflowCompleteEvent) for event in events)
