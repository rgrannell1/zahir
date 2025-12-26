
from zahir.types import Context, EventRegistry
from zahir.scope import LocalScope
from zahir.logging import ZahirLogger

class DummyEventRegistry(EventRegistry):
    def register(self, event): pass

class DummyLogger(ZahirLogger):
    def __init__(self, event_registry, job_registry):
        super().__init__(event_registry, job_registry)
    def render(self, context): pass
import os
import tempfile
import pytest
from zahir.job_registry.sqlite import SQLiteJobRegistry
from zahir.types import Job, JobState, JobInformation
from zahir.dependencies.group import DependencyGroup
from zahir.events import WorkflowOutputEvent


class DummyJob(Job):
    def __init__(self, input=None, dependencies=None, options=None, job_id=None, parent_id=None):
        self.input = input if input is not None else {}
        self.dependencies = dependencies if dependencies is not None else DependencyGroup({})
        self.options = options
        self.job_id = job_id if job_id is not None else "dummy"
        self.parent_id = parent_id

    @classmethod
    def run(cls, context, input, dependencies):
        yield WorkflowOutputEvent({"result": 42})

def test_sqlite_job_registry_lifecycle():
    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        db_path = tmp.name
    try:
        registry = SQLiteJobRegistry(db_path)
        job = DummyJob(job_id="job1")
        job_id = registry.add(job)
        assert job_id == "job1"
        assert registry.get_state(job_id) == JobState.PENDING
        registry.set_state(job_id, JobState.COMPLETED)
        assert registry.get_state(job_id) == JobState.COMPLETED
        registry.set_output(job_id, {"result": 42})
        output = registry.get_output(job_id)
        assert output is not None, "Output should not be None"
        assert output["result"] == 42

        # Use a real Context with dummy event registry and logger
        scope = LocalScope(jobs=[DummyJob])
        event_registry = DummyEventRegistry()
        dummy_context = Context(
            scope=scope,
            job_registry=registry,
            event_registry=event_registry,
            logger=DummyLogger(event_registry, registry)
        )
        info = list(registry.jobs(dummy_context))
        assert any(j.job_id == job_id for j in info)
    finally:
        os.remove(db_path)
