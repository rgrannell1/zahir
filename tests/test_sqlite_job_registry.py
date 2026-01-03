from datetime import UTC, datetime, timedelta
import multiprocessing
import pathlib
import tempfile

from zahir.base_types import Context, EventRegistry, Job, JobState, JobTimingInformation
from zahir.dependencies.group import DependencyGroup
from zahir.events import WorkflowOutputEvent
from zahir.exception import DuplicateJobError, MissingJobError
from zahir.job_registry.sqlite import SQLiteJobRegistry
from zahir.scope import LocalScope


class DummyEventRegistry(EventRegistry):
    def __init__(self):
        self.queue = multiprocessing.Queue()

    def register(self, event):
        self.queue.put(event)


class DummyJob(Job):
    def __init__(self, input=None, dependencies=None, options=None, job_id=None, parent_id=None):
        self.input = input if input is not None else {}
        self.dependencies = dependencies if dependencies is not None else DependencyGroup({})
        self.job_options = options
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
        registry.init("test-worker")
        job = DummyJob(job_id="job1")

        dummy_queue = multiprocessing.Queue()
        job_id = registry.add(job, dummy_queue)
        assert job_id == "job1"
        assert registry.get_state(job_id) == JobState.READY
        registry.set_state(job_id, "wf-test", dummy_queue, JobState.COMPLETED)
        assert registry.get_state(job_id) == JobState.COMPLETED
        registry.set_output(job_id, "wf-test", dummy_queue, {"result": 42})
        output = registry.get_output(job_id)
        assert output is not None, "Output should not be None"
        assert output["result"] == 42

        # Use a real Context with dummy event registry and logger
        scope = LocalScope(jobs=[DummyJob])
        dummy_context = Context(scope=scope, job_registry=registry)
        info = list(registry.jobs(dummy_context))
        assert any(j.job_id == job_id for j in info)
    finally:
        registry.conn.close()
        pathlib.Path(db_path).unlink()


def test_job_timing_information_time_since_started():
    """Test JobTimingInformation.time_since_started with started_at set."""
    started = datetime.now(tz=UTC) - timedelta(seconds=10)
    timing = JobTimingInformation(
        started_at=started,
        recovery_started_at=None,
        completed_at=None,
    )
    elapsed = timing.time_since_started()
    assert isinstance(elapsed, float)


def test_job_timing_information_time_since_started_none():
    """Test JobTimingInformation.time_since_started with started_at=None."""
    timing = JobTimingInformation(
        started_at=None,
        recovery_started_at=None,
        completed_at=None,
    )
    assert timing.time_since_started() is None


def test_job_timing_information_time_since_recovery_started():
    """Test JobTimingInformation.time_since_recovery_started with recovery_started_at set."""
    recovery_started = datetime.now(tz=UTC) - timedelta(seconds=5)
    timing = JobTimingInformation(
        started_at=None,
        recovery_started_at=recovery_started,
        completed_at=None,
    )
    elapsed = timing.time_since_recovery_started()
    assert isinstance(elapsed, float)


def test_job_timing_information_time_since_recovery_started_none():
    """Test JobTimingInformation.time_since_recovery_started with recovery_started_at=None."""
    timing = JobTimingInformation(
        started_at=None,
        recovery_started_at=None,
        completed_at=None,
    )
    assert timing.time_since_recovery_started() is None


def test_delete_claims():
    """Test deleting all job claims."""
    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        db_path = tmp.name
    try:
        registry = SQLiteJobRegistry(db_path)
        registry.init("worker-1")
        job = DummyJob(job_id="job-claim-1")
        dummy_queue = multiprocessing.Queue()
        registry.add(job, dummy_queue)
        registry.set_claim("job-claim-1", "worker-1")
        registry.delete_claims()
    finally:
        registry.conn.close()
        pathlib.Path(db_path).unlink()


def test_set_claim():
    """Test setting a claim on a job."""
    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        db_path = tmp.name
    try:
        registry = SQLiteJobRegistry(db_path)
        registry.init("worker-2")
        job = DummyJob(job_id="job-claim-2")
        dummy_queue = multiprocessing.Queue()
        registry.add(job, dummy_queue)
        result = registry.set_claim("job-claim-2", "worker-2")
        assert isinstance(result, bool)
    finally:
        registry.conn.close()
        pathlib.Path(db_path).unlink()


def test_claim_job():
    """Test claiming a ready job."""
    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        db_path = tmp.name
    try:
        registry = SQLiteJobRegistry(db_path)
        registry.init("worker-3")
        job = DummyJob(job_id="job-claim-3")
        dummy_queue = multiprocessing.Queue()
        registry.add(job, dummy_queue)
        scope = LocalScope(jobs=[DummyJob])
        context = Context(scope=scope, job_registry=registry)
        claimed = registry.claim(context, "worker-3")
        assert isinstance(claimed, (DummyJob, type(None)))
    finally:
        registry.conn.close()
        pathlib.Path(db_path).unlink()


def test_claim_no_jobs():
    """Test claiming when no jobs are available."""
    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        db_path = tmp.name
    try:
        registry = SQLiteJobRegistry(db_path)
        registry.init("worker-4")
        scope = LocalScope(jobs=[DummyJob])
        context = Context(scope=scope, job_registry=registry)
        claimed = registry.claim(context, "worker-4")
        assert claimed is None
    finally:
        registry.conn.close()
        pathlib.Path(db_path).unlink()


def test_get_job_timing():
    """Test getting job timing information."""
    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        db_path = tmp.name
    try:
        registry = SQLiteJobRegistry(db_path)
        registry.init("worker-5")
        job = DummyJob(job_id="job-timing-1")
        dummy_queue = multiprocessing.Queue()
        registry.add(job, dummy_queue)
        registry.set_state("job-timing-1", "wf-1", dummy_queue, JobState.RUNNING)
        timing = registry.get_job_timing("job-timing-1")
        assert isinstance(timing, JobTimingInformation)
    finally:
        registry.conn.close()
        pathlib.Path(db_path).unlink()


def test_get_job_timing_missing_job():
    """Test getting timing for non-existent job."""
    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        db_path = tmp.name
    try:
        registry = SQLiteJobRegistry(db_path)
        registry.init("worker-6")
        try:
            registry.get_job_timing("nonexistent")
            assert False, "Should have raised MissingJobError"
        except MissingJobError:
            pass
    finally:
        registry.conn.close()
        pathlib.Path(db_path).unlink()


def test_is_finished():
    """Test checking if a job is finished."""
    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        db_path = tmp.name
    try:
        registry = SQLiteJobRegistry(db_path)
        registry.init("worker-7")
        job = DummyJob(job_id="job-finished-1")
        dummy_queue = multiprocessing.Queue()
        registry.add(job, dummy_queue)
        assert not registry.is_finished("job-finished-1")
        registry.set_state("job-finished-1", "wf-1", dummy_queue, JobState.COMPLETED)
        assert registry.is_finished("job-finished-1")
    finally:
        registry.conn.close()
        pathlib.Path(db_path).unlink()


def test_add_duplicate_job():
    """Test adding a duplicate job raises error."""
    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        db_path = tmp.name
    try:
        registry = SQLiteJobRegistry(db_path)
        registry.init("worker-8")
        job = DummyJob(job_id="job-dup")
        dummy_queue = multiprocessing.Queue()
        registry.add(job, dummy_queue)
        try:
            registry.add(job, dummy_queue)
            assert False, "Should have raised DuplicateJobError"
        except DuplicateJobError:
            pass
    finally:
        registry.conn.close()
        pathlib.Path(db_path).unlink()


def test_get_state_missing_job():
    """Test getting state of non-existent job."""
    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        db_path = tmp.name
    try:
        registry = SQLiteJobRegistry(db_path)
        registry.init("worker-9")
        try:
            registry.get_state("nonexistent")
            assert False, "Should have raised MissingJobError"
        except MissingJobError:
            pass
    finally:
        registry.conn.close()
        pathlib.Path(db_path).unlink()


def test_set_state_with_error():
    """Test setting state with an error."""
    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        db_path = tmp.name
    try:
        registry = SQLiteJobRegistry(db_path)
        registry.init("worker-10")
        job = DummyJob(job_id="job-error-1")
        dummy_queue = multiprocessing.Queue()
        registry.add(job, dummy_queue)
        err = ValueError("test error")
        registry.set_state("job-error-1", "wf-1", dummy_queue, JobState.IRRECOVERABLE, error=err)
    finally:
        registry.conn.close()
        pathlib.Path(db_path).unlink()


def test_add_error():
    """Test adding an error to a job."""
    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        db_path = tmp.name
    try:
        registry = SQLiteJobRegistry(db_path)
        registry.init("worker-11")
        job = DummyJob(job_id="job-error-2")
        dummy_queue = multiprocessing.Queue()
        registry.add(job, dummy_queue)
        err = RuntimeError("test error")
        registry.add_error("job-error-2", err)
    finally:
        registry.conn.close()
        pathlib.Path(db_path).unlink()


def test_get_errors():
    """Test getting errors for a job."""
    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        db_path = tmp.name
    try:
        registry = SQLiteJobRegistry(db_path)
        registry.init("worker-12")
        job = DummyJob(job_id="job-error-3")
        dummy_queue = multiprocessing.Queue()
        registry.add(job, dummy_queue)
        err = RuntimeError("test error")
        registry.add_error("job-error-3", err)
        errors = registry.get_errors("job-error-3")
        assert isinstance(errors, list)
    finally:
        registry.conn.close()
        pathlib.Path(db_path).unlink()


def test_is_active():
    """Test checking if any jobs are active."""
    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        db_path = tmp.name
    try:
        registry = SQLiteJobRegistry(db_path)
        registry.init("worker-13")
        job = DummyJob(job_id="job-active-1")
        dummy_queue = multiprocessing.Queue()
        registry.add(job, dummy_queue)
        assert registry.is_active()
        registry.set_state("job-active-1", "wf-1", dummy_queue, JobState.COMPLETED)
        assert not registry.is_active()
    finally:
        registry.conn.close()
        pathlib.Path(db_path).unlink()


def test_jobs_with_state_filter():
    """Test retrieving jobs with state filter."""
    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        db_path = tmp.name
    try:
        registry = SQLiteJobRegistry(db_path)
        registry.init("worker-14")
        job1 = DummyJob(job_id="job-filter-1")
        job2 = DummyJob(job_id="job-filter-2")
        dummy_queue = multiprocessing.Queue()
        registry.add(job1, dummy_queue)
        registry.add(job2, dummy_queue)
        registry.set_state("job-filter-1", "wf-1", dummy_queue, JobState.COMPLETED)
        scope = LocalScope(jobs=[DummyJob])
        context = Context(scope=scope, job_registry=registry)
        completed_jobs = list(registry.jobs(context, state=JobState.COMPLETED))
        assert len(completed_jobs) == 1
    finally:
        registry.conn.close()
        pathlib.Path(db_path).unlink()


def test_get_output_none():
    """Test getting output when none exists."""
    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        db_path = tmp.name
    try:
        registry = SQLiteJobRegistry(db_path)
        registry.init("worker-15")
        job = DummyJob(job_id="job-output-1")
        dummy_queue = multiprocessing.Queue()
        registry.add(job, dummy_queue)
        output = registry.get_output("job-output-1")
        assert output is None
    finally:
        registry.conn.close()
        pathlib.Path(db_path).unlink()


def test_set_state_running():
    """Test setting job to RUNNING state sets start time."""
    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        db_path = tmp.name
    try:
        registry = SQLiteJobRegistry(db_path)
        registry.init("worker-16")
        job = DummyJob(job_id="job-running-1")
        dummy_queue = multiprocessing.Queue()
        registry.add(job, dummy_queue)
        registry.set_state("job-running-1", "wf-1", dummy_queue, JobState.RUNNING)
        timing = registry.get_job_timing("job-running-1")
        assert timing.started_at is not None
    finally:
        registry.conn.close()
        pathlib.Path(db_path).unlink()


def test_set_state_recovering():
    """Test setting job to RECOVERING state sets recovery start time."""
    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        db_path = tmp.name
    try:
        registry = SQLiteJobRegistry(db_path)
        registry.init("worker-17")
        job = DummyJob(job_id="job-recovery-1")
        dummy_queue = multiprocessing.Queue()
        registry.add(job, dummy_queue)
        registry.set_state("job-recovery-1", "wf-1", dummy_queue, JobState.RECOVERING)
        timing = registry.get_job_timing("job-recovery-1")
        assert timing.recovery_started_at is not None
    finally:
        registry.conn.close()
        pathlib.Path(db_path).unlink()
