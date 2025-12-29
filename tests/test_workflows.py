from collections.abc import Iterator, Mapping
import pathlib
import re
import tempfile
from typing import cast

from zahir.base_types import Context, Dependency, Job
from zahir.context import MemoryContext
from zahir.dependencies.group import DependencyGroup
from zahir.dependencies.job import JobDependency
from zahir.events import Await, JobOutputEvent, WorkflowOutputEvent
from zahir.job_registry import SQLiteJobRegistry
from zahir.scope import LocalScope
from zahir.tasks.decorator import job
from zahir.worker import LocalWorkflow


@job
def AddJob(cls, context: Context, input, dependencies):
    return JobOutputEvent({"count": input["count"] + 1})


@job
def YieldMany(cls, context: Context, input, dependencies):
    """Interyield to another job"""

    count = 0
    result = yield Await(AddJob({"count": count}, {}))
    count = result["count"]
    result = yield Await(AddJob({"count": count}, {}))

    return JobOutputEvent({"count": count})


def test_workflow_yield_many():
    tmp_file = "/tmp/zahir_yield_many.db"
    pathlib.Path(tmp_file).touch(exist_ok=True)

    context = MemoryContext(
        scope=LocalScope(jobs=[
            AddJob,
            YieldMany,
        ]), job_registry=SQLiteJobRegistry(tmp_file)
    )

    workflow = LocalWorkflow(context)

    yield_many = YieldMany({}, {})
    events = list(workflow.run(yield_many))

    print(events)

test_workflow_yield_many()
