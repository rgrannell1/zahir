import pathlib
import tempfile

from zahir.base_types import Context, EventRegistry, Job, JobState
from zahir.dependencies.group import DependencyGroup
from zahir.events import WorkflowOutputEvent
from zahir.job_registry.sqlite import SQLiteJobRegistry
from zahir.logging import ZahirLogger
from zahir.scope import LocalScope


class DummyEventRegistry(EventRegistry):
    def register(self, event):
        pass


class DummyLogger(ZahirLogger):
    def __init__(self, job_registry):
        super().__init__(job_registry)

    def render(self, context):
        pass


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
        assert registry.get_state(job_id) == JobState.READY
        registry.set_state(job_id, JobState.COMPLETED)
        assert registry.get_state(job_id) == JobState.COMPLETED
        registry.set_output(job_id, {"result": 42})
        output = registry.get_output(job_id)
        assert output is not None, "Output should not be None"
        assert output["result"] == 42

        # Use a real Context with dummy event registry and logger
        scope = LocalScope(jobs=[DummyJob])
        dummy_context = Context(
            scope=scope,
            job_registry=registry,
            logger=DummyLogger(registry),
        )
        info = list(registry.jobs(dummy_context))
        assert any(j.job_id == job_id for j in info)
    finally:
        pathlib.Path(db_path).unlink()
