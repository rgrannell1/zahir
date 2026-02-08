"""Tests for output logging functionality with PID prepending."""

import pathlib
import sys
import tempfile

from zahir.base_types import Context
from zahir.context.memory import MemoryContext
from zahir.events import JobOutputEvent
from zahir.job_registry.sqlite import SQLiteJobRegistry
from zahir.jobs.decorator import spec
from zahir.scope import LocalScope
from zahir.worker import LocalWorkflow


def make_context() -> MemoryContext:
    """Create a MemoryContext with a file-based SQLite registry (required for cross-process workflows)."""
    tmp = tempfile.NamedTemporaryFile(delete=False)  # noqa: SIM115
    scope = LocalScope.from_module(sys.modules[__name__])
    job_registry = SQLiteJobRegistry(tmp.name)
    return MemoryContext(scope=scope, job_registry=job_registry)


@spec()
def SimpleLoggingJob(context: Context, input_dict, dependencies):
    """A test job that writes to stdout and stderr."""
    print("This is stdout output")
    sys.stderr.write("This is stderr output\n")
    print(f"Input was: {input_dict}")
    yield JobOutputEvent({"result": "success"})


@spec()
def VerboseLoggingJob(context: Context, input_dict, dependencies):
    """A test job with multiple lines of output."""
    for idx in range(3):
        print(f"Line {idx} to stdout")
        sys.stderr.write(f"Line {idx} to stderr\n")
    yield JobOutputEvent({"lines": 3})


# Unique markers so we can prove job output is captured (not from any other code path)
STDOUT_CAPTURE_MARKER = "STDOUT_CAPTURED_BY_ZAHIR_TEST_8f3a"
STDERR_CAPTURE_MARKER = "STDERR_CAPTURED_BY_ZAHIR_TEST_8f3a"


@spec()
def CaptureProofJob(context: Context, input_dict, dependencies):
    """Job that prints unique markers so we can assert stdout/stderr are captured."""
    print(STDOUT_CAPTURE_MARKER)
    sys.stdout.flush()
    sys.stderr.write(STDERR_CAPTURE_MARKER + "\n")
    sys.stderr.flush()
    yield JobOutputEvent({"ok": True})


def test_setup_output_logging_captures_after_fork():
    """Prove that after fork, setup_output_logging + print in child ends up in the log file."""
    import multiprocessing

    def child_proc(log_dir: str, marker: str, result_queue: multiprocessing.Queue) -> None:
        from zahir.utils.output_logging import setup_output_logging

        setup_output_logging(log_dir=log_dir)
        print(marker)
        sys.stdout.flush()
        # Signal we're done so parent can read the file
        result_queue.put(None)

    with tempfile.TemporaryDirectory() as temp_dir:
        result_queue = multiprocessing.Queue()
        proc = multiprocessing.Process(
            target=child_proc,
            args=(temp_dir, STDOUT_CAPTURE_MARKER, result_queue),
        )
        proc.start()
        result_queue.get(timeout=5)
        proc.join(timeout=5)

        stdout_log = pathlib.Path(temp_dir) / "stdout.log"
        assert stdout_log.exists(), f"stdout.log not found in {temp_dir}"
        all_stdout = stdout_log.read_text()
        assert STDOUT_CAPTURE_MARKER in all_stdout, (
            f"Marker not in stdout after fork+setup_output_logging+print. Content: {all_stdout!r}"
        )


def test_workflow_captures_job_stdout_and_stderr():
    """Prove that print() and stderr from a job spec end up in the log files."""
    context = make_context()
    with tempfile.TemporaryDirectory() as temp_dir:
        workflow = LocalWorkflow(context=context, log_output_dir=temp_dir, start_job_type="CaptureProof", max_workers=2)
        job = CaptureProofJob({}, {})
        list(workflow.run(job, show_progress=False, events_filter=None))

        log_root = pathlib.Path(temp_dir)
        # One subdirectory per workflow run, all PID log files inside it
        subdirs = [entry for entry in log_root.iterdir() if entry.is_dir()]
        assert len(subdirs) == 1, f"Expected exactly 1 log subdirectory, found {len(subdirs)}: {subdirs}"

        log_subdir = subdirs[0]
        stdout_log = log_subdir / "stdout.log"
        stderr_log = log_subdir / "stderr.log"
        assert stdout_log.exists(), f"stdout.log not found in {log_subdir}"
        assert stderr_log.exists(), f"stderr.log not found in {log_subdir}"

        all_stdout = stdout_log.read_text()
        all_stderr = stderr_log.read_text()

        assert STDOUT_CAPTURE_MARKER in all_stdout, (
            f"Job stdout marker not found in stdout.log. Content: {all_stdout!r}"
        )
        assert STDERR_CAPTURE_MARKER in all_stderr, (
            f"Job stderr marker not found in stderr.log. Content: {all_stderr!r}"
        )
        # Prove it went through our PID-prepending writer
        assert "[PID" in all_stdout and STDOUT_CAPTURE_MARKER in all_stdout
        assert "[PID" in all_stderr and STDERR_CAPTURE_MARKER in all_stderr


def test_output_logging_with_log_directory():
    """Test that output logging creates files with PID prepending."""
    context = make_context()
    with tempfile.TemporaryDirectory() as temp_dir:
        workflow = LocalWorkflow(context=context, log_output_dir=temp_dir, start_job_type="TestJob")
        job = SimpleLoggingJob({}, {})
        list(workflow.run(job, show_progress=False, events_filter=None))

        # One subdirectory per workflow run
        log_dir = pathlib.Path(temp_dir)
        subdirs = [entry for entry in log_dir.iterdir() if entry.is_dir()]
        assert len(subdirs) == 1, f"Expected exactly 1 subdirectory, found {len(subdirs)}: {subdirs}"

        log_subdir = subdirs[0]
        assert log_subdir.name.startswith("TestJob-"), f"Subdirectory should start with TestJob-: {log_subdir.name}"

        # All workers write to the same stdout.log / stderr.log
        stdout_log = log_subdir / "stdout.log"
        stderr_log = log_subdir / "stderr.log"
        assert stdout_log.exists(), "stdout.log not found"
        assert stderr_log.exists(), "stderr.log not found"

        # Verify PID prepending in output
        all_stdout = stdout_log.read_text()
        assert "[PID" in all_stdout and "This is stdout output" in all_stdout, (
            f"Expected PID-prepended stdout output, got: {all_stdout!r}"
        )


def test_output_logging_without_job_type():
    """Test that output logging works when start_job_type is None."""
    context = make_context()
    with tempfile.TemporaryDirectory() as temp_dir:
        workflow = LocalWorkflow(context=context, log_output_dir=temp_dir)
        job = SimpleLoggingJob({}, {})
        list(workflow.run(job, show_progress=False, events_filter=None))

        # Single log subdirectory (timestamp only, no job type prefix)
        log_dir = pathlib.Path(temp_dir)
        subdirs = [entry for entry in log_dir.iterdir() if entry.is_dir()]
        assert len(subdirs) == 1, f"Expected exactly 1 subdirectory, found {len(subdirs)}"


def test_output_logging_multiple_lines():
    """Test that PID prepending works across multiple lines."""
    context = make_context()
    with tempfile.TemporaryDirectory() as temp_dir:
        workflow = LocalWorkflow(context=context, log_output_dir=temp_dir, start_job_type="VerboseJob")
        job = VerboseLoggingJob({}, {})
        list(workflow.run(job, show_progress=False, events_filter=None))

        # Single log subdirectory per workflow
        log_dir = pathlib.Path(temp_dir)
        subdirs = [entry for entry in log_dir.iterdir() if entry.is_dir()]
        assert len(subdirs) == 1, f"Expected 1 VerboseJob subdirectory, found {len(subdirs)}"
        assert subdirs[0].name.startswith("VerboseJob-")

        log_subdir = subdirs[0]
        stdout_log = log_subdir / "stdout.log"
        assert stdout_log.exists(), "stdout.log not found"
        all_stdout = stdout_log.read_text()

        assert "Line" in all_stdout, f"Expected job output in stdout logs, got: {all_stdout!r}"
        for line in all_stdout.strip().split("\n"):
            if line:
                assert line.startswith("[PID"), f"Line doesn't have PID prefix: {line}"


def test_pid_prepending_writer_empty_text():
    """Test PIDPrependingWriter.write() with empty text."""
    import io

    from zahir.utils.output_logging import PIDPrependingWriter

    file_obj = io.StringIO()
    writer = PIDPrependingWriter(file_obj, process_id=12345)

    # Writing empty string should return 0
    result = writer.write("")
    assert result == 0
    assert file_obj.getvalue() == ""

    # Writing None-like empty string
    result = writer.write("")
    assert result == 0


def test_pid_prepending_writer_flush_with_buffer():
    """Test PIDPrependingWriter.flush() with remaining buffer."""
    import io

    from zahir.utils.output_logging import PIDPrependingWriter

    file_obj = io.StringIO()
    writer = PIDPrependingWriter(file_obj, process_id=99999)

    # Write partial line (no newline)
    writer.write("partial line")
    assert file_obj.getvalue() == ""

    # Flush should write the buffered content
    writer.flush()
    assert "[PID 99999] partial line\n" in file_obj.getvalue()


def test_pid_prepending_writer_multiple_writes():
    """Test PIDPrependingWriter with multiple writes that build up lines."""
    import io

    from zahir.utils.output_logging import PIDPrependingWriter

    file_obj = io.StringIO()
    writer = PIDPrependingWriter(file_obj, process_id=11111)

    # Write in chunks
    writer.write("line 1\n")
    writer.write("line 2")
    writer.write(" continued\n")
    writer.write("line 3\n")

    content = file_obj.getvalue()
    assert "[PID 11111] line 1\n" in content
    assert "[PID 11111] line 2 continued\n" in content
    assert "[PID 11111] line 3\n" in content


def test_setup_output_logging_returns_none_when_no_dir():
    """Test setup_output_logging returns None, None when log_dir is None."""
    from zahir.utils.output_logging import setup_output_logging

    stdout_path, stderr_path = setup_output_logging(log_dir=None)
    assert stdout_path is None
    assert stderr_path is None


def test_create_workflow_log_dir():
    """Test create_workflow_log_dir creates a properly named subdirectory."""
    import tempfile

    from zahir.utils.output_logging import create_workflow_log_dir

    with tempfile.TemporaryDirectory() as temp_dir:
        # Test with job type
        path = create_workflow_log_dir(temp_dir, start_job_type="TestJob")
        assert path.parent == pathlib.Path(temp_dir)
        assert path.name.startswith("TestJob-")
        assert path.is_dir()

        # Test without job type
        path2 = create_workflow_log_dir(temp_dir, start_job_type=None)
        assert path2.parent == pathlib.Path(temp_dir)
        assert not path2.name.startswith("TestJob-")
        assert path2.is_dir()

        # Symlinks in the root dir should point to the most recent run's log files
        log_root = pathlib.Path(temp_dir)
        latest_stdout = log_root / "latest.stdout"
        latest_stderr = log_root / "latest.stderr"

        assert latest_stdout.is_symlink()
        assert latest_stderr.is_symlink()
        assert latest_stdout.resolve() == (path2 / "stdout.log").resolve()
        assert latest_stderr.resolve() == (path2 / "stderr.log").resolve()


def test_setup_output_logging_creates_files():
    """Test setup_output_logging actually creates log files."""
    import multiprocessing
    import tempfile

    from zahir.utils.output_logging import setup_output_logging

    def child_creates_files(log_dir: str, result_queue: multiprocessing.Queue) -> None:
        stdout_path, stderr_path = setup_output_logging(log_dir=log_dir)
        result_queue.put((str(stdout_path), str(stderr_path)))

    with tempfile.TemporaryDirectory() as temp_dir:
        result_queue = multiprocessing.Queue()
        proc = multiprocessing.Process(target=child_creates_files, args=(temp_dir, result_queue))
        proc.start()
        stdout_str, stderr_str = result_queue.get(timeout=5)
        proc.join(timeout=5)

        stdout_path = pathlib.Path(stdout_str)
        stderr_path = pathlib.Path(stderr_str)

        assert stdout_path.exists()
        assert stderr_path.exists()
        assert stdout_path.name == "stdout.log"
        assert stderr_path.name == "stderr.log"
        assert stdout_path.parent == pathlib.Path(temp_dir)
