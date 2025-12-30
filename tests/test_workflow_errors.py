import tempfile

import pytest

from zahir.base_types import Context
from zahir.context import MemoryContext
from zahir.events import (
    ZahirCustomEvent,
)
from zahir.job_registry import SQLiteJobRegistry
from zahir.scope import LocalScope
from zahir.tasks.decorator import job
from zahir.worker import LocalWorkflow


@job
def JustReturns(cls, context: Context, input, dependencies):
    """Interyield to another job"""

    return ZahirCustomEvent(output={"message": "This should be seen"})


def test_accidental_return():
    """Prove that no events are consumed after job output is returned. Also document the normal execution flow."""

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    context = MemoryContext(scope=LocalScope(jobs=[JustReturns]), job_registry=SQLiteJobRegistry(tmp_file))
    workflow = LocalWorkflow(context)

    job = JustReturns({}, {})
    with pytest.raises(TypeError):
        list(workflow.run(job, all_events=True))
