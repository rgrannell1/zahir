"""Integration test for context queue functionality with parent-child job communication."""

import tempfile

from zahir.base_types import Context
from zahir.context import MemoryContext
from zahir.events import Await, JobOutputEvent
from zahir.job_registry import SQLiteJobRegistry
from zahir.jobs.decorator import spec
from zahir.scope import LocalScope
from zahir.worker import LocalWorkflow
import sys


@spec()
def ChildJobA(spec_args, context: Context, input, dependencies):
    """Child job A - sends marco and receives polo."""
    queue_a_id = input["queue_a_id"]
    queue_b_id = input["queue_b_id"]

    queue_a = context.get_queue(queue_a_id)
    queue_b = context.get_queue(queue_b_id)

    # Send "Marco" on queue_a
    queue_a.put({"message": "marco", "from": "A"})

    # Wait for "Polo" response on queue_b
    response = queue_b.get(timeout=5)
    assert response["message"] == "polo"
    assert response["from"] == "B"

    yield JobOutputEvent({"child": "A", "result": "success"})


@spec()
def ChildJobB(spec_args, context: Context, input, dependencies):
    """Child job B - receives marco and sends polo."""
    queue_a_id = input["queue_a_id"]
    queue_b_id = input["queue_b_id"]

    queue_a = context.get_queue(queue_a_id)
    queue_b = context.get_queue(queue_b_id)

    # Wait for "Marco" on queue_a
    marco = queue_a.get(timeout=5)
    assert marco["message"] == "marco"
    assert marco["from"] == "A"

    # Send "Polo" response on queue_b
    queue_b.put({"message": "polo", "from": "B"})

    yield JobOutputEvent({"child": "B", "result": "success"})


@spec()
def ParentJob(spec_args, context: Context, input, dependencies):
    """Parent job that creates queues and launches children."""
    # Create two queues for communication
    queue_a_id, queue_a = context.add_queue()
    queue_b_id, queue_b = context.add_queue()

    # Launch child jobs with queue IDs
    results = yield Await([
        ChildJobA({"queue_a_id": queue_a_id, "queue_b_id": queue_b_id}, {}),
        ChildJobB({"queue_a_id": queue_a_id, "queue_b_id": queue_b_id}, {}),
    ])

    # Verify both children completed successfully
    assert len(results) == 2
    assert results[0]["child"] == "A"
    assert results[0]["result"] == "success"
    assert results[1]["child"] == "B"
    assert results[1]["result"] == "success"

    yield JobOutputEvent({"parent": "complete", "children_communicated": True})


def test_parent_child_jobs_marco_polo_via_queues():
    """Test that parent job can launch children that communicate via context queues.

    This demonstrates:
    1. Parent creates two queues with context.add_queue()
    2. Parent launches two child jobs, passing queue IDs
    3. Child A sends "marco" on queue_a
    4. Child B receives "marco" and responds with "polo"
    5. Child A receives "polo"
    6. Both jobs complete successfully
    """


def test_parent_child_jobs_marco_polo_via_queues():
    """Test that parent job can launch children that communicate via context queues.

    This demonstrates:
    1. Parent creates two queues with context.add_queue()
    2. Parent launches two child jobs, passing queue IDs
    3. Child A sends "marco" on queue_a
    4. Child B receives "marco" and responds with "polo"
    5. Child A receives "polo"
    6. Both jobs complete successfully
    """
    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    # Run the workflow
    scope = LocalScope.from_module(sys.modules[__name__])
    registry = SQLiteJobRegistry(tmp_file)
    registry.init("test-worker")
    context = MemoryContext(scope=scope, job_registry=registry)

    workflow = LocalWorkflow(context, max_workers=4)
    parent_job_instance = ParentJob({}, {})
    events = list(workflow.run(parent_job_instance, events_filter=None))

    # Extract JobOutputEvent and check for results
    output_events = [e for e in events if isinstance(e, JobOutputEvent)]

    # Should have outputs from both children and the parent
    assert len(output_events) >= 3, (
        f"Expected at least 3 output events, got {len(output_events)}: {[type(e).__name__ for e in events]}"
    )

    # Find the parent job output
    parent_output = None
    for output_event in output_events:
        if hasattr(output_event, "output") and output_event.output.get("parent") == "complete":
            parent_output = output_event.output
            break

    assert parent_output is not None, "Parent output event not found"
    assert parent_output["children_communicated"] is True
