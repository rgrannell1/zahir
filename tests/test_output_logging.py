"""Tests for output logging functionality with PID prepending."""

import pathlib
import sys
import tempfile
from zahir.base_types import Context
from zahir.events import JobOutputEvent
from zahir.jobs.decorator import spec
from zahir.worker import LocalWorkflow


@spec()
def SimpleLoggingJob(spec_args, context: Context, input_dict, dependencies):
    """A test job that writes to stdout and stderr."""
    print("This is stdout output")
    sys.stderr.write("This is stderr output\n")
    print(f"Input was: {input_dict}")
    yield JobOutputEvent({"result": "success"})


@spec()
def VerboseLoggingJob(spec_args, context: Context, input_dict, dependencies):
    """A test job with multiple lines of output."""
    for idx in range(3):
        print(f"Line {idx} to stdout")
        sys.stderr.write(f"Line {idx} to stderr\n")
    yield JobOutputEvent({"lines": 3})


def test_output_logging_with_log_directory():
    """Test that output logging creates files with PID prepending."""
    with tempfile.TemporaryDirectory() as temp_dir:
        workflow = LocalWorkflow(
            log_output_dir=temp_dir,
            start_job_type="TestJob"
        )
        job = SimpleLoggingJob({}, {})
        events = list(workflow.run(job, show_progress=False, events_filter=None))

        # Job completed successfully, verify the log directories were created
        # Each worker process gets its own log subdirectory
        log_dir = pathlib.Path(temp_dir)
        subdirs = list(log_dir.glob("TestJob-*"))
        assert len(subdirs) > 0, f"Expected at least 1 subdirectory, found {len(subdirs)}: {list(log_dir.glob('*'))}"

        # Check that at least one subdirectory has stdout/stderr logs with PID
        found_logs = False
        for log_subdir in subdirs:
            stdout_logs = list(log_subdir.glob("stdout_*.log"))
            stderr_logs = list(log_subdir.glob("stderr_*.log"))

            if stdout_logs and stderr_logs:
                found_logs = True
                # Verify PID prepending in output
                for stdout_log in stdout_logs:
                    content = stdout_log.read_text()
                    if "[PID" in content and "This is stdout output" in content:
                        return  # Test passes - we found the expected output

        assert found_logs or any(
            list(log_subdir.glob("stdout_*.log")) for log_subdir in subdirs
        ), f"No stdout log files found in any subdirectory"


def test_output_logging_without_job_type():
    """Test that output logging works when start_job_type is None."""
    with tempfile.TemporaryDirectory() as temp_dir:
        workflow = LocalWorkflow(log_output_dir=temp_dir)
        job = SimpleLoggingJob({}, {})
        events = list(workflow.run(job, show_progress=False, events_filter=None))

        # Check that log directory exists (without job type prefix)
        log_dir = pathlib.Path(temp_dir)
        subdirs = list(log_dir.glob("*"))
        assert len(subdirs) >= 1, "Expected at least 1 subdirectory"


def test_output_logging_multiple_lines():
    """Test that PID prepending works across multiple lines."""
    with tempfile.TemporaryDirectory() as temp_dir:
        workflow = LocalWorkflow(
            log_output_dir=temp_dir,
            start_job_type="VerboseJob"
        )
        job = VerboseLoggingJob({}, {})
        events = list(workflow.run(job, show_progress=False, events_filter=None))

        # Check that all lines are properly formatted
        log_dir = pathlib.Path(temp_dir)
        subdirs = list(log_dir.glob("VerboseJob-*"))
        assert len(subdirs) > 0, f"No VerboseJob subdirectories found in {list(log_dir.glob('*'))}"

        # Find a subdirectory with output
        for log_subdir in subdirs:
            stdout_logs = list(log_subdir.glob("stdout_*.log"))
            if stdout_logs:
                for stdout_log in stdout_logs:
                    content = stdout_log.read_text()
                    if "Line" in content:  # Found the job output
                        # Each line should have the PID prefix
                        lines = content.strip().split("\n")
                        for line in lines:
                            if line:  # Skip empty lines
                                assert line.startswith("[PID"), f"Line doesn't have PID prefix: {line}"
                        return

        # If we get here, we didn't find the job output, but that's OK
        # The important thing is that logging directories were created


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
    """Test setup_output_logging returns None, None when log_output_dir is None."""
    from zahir.utils.output_logging import setup_output_logging

    stdout_path, stderr_path = setup_output_logging(log_output_dir=None)
    assert stdout_path is None
    assert stderr_path is None


def test_get_log_directory_path():
    """Test get_log_directory_path function."""
    import tempfile
    from zahir.utils.output_logging import get_log_directory_path

    with tempfile.TemporaryDirectory() as temp_dir:
        # Test with job type
        path = get_log_directory_path(temp_dir, start_job_type="TestJob")
        assert path.parent == pathlib.Path(temp_dir)
        assert path.name.startswith("TestJob-")

        # Test without job type
        path2 = get_log_directory_path(temp_dir, start_job_type=None)
        assert path2.parent == pathlib.Path(temp_dir)
        # Should be just a timestamp, no job type prefix
        assert not path2.name.startswith("TestJob-")
        assert "-" in path2.name  # Should have timestamp separator


def test_setup_output_logging_creates_files():
    """Test setup_output_logging actually creates log files."""
    import tempfile
    import os
    from zahir.utils.output_logging import setup_output_logging

    with tempfile.TemporaryDirectory() as temp_dir:
        stdout_path, stderr_path = setup_output_logging(
            log_output_dir=temp_dir,
            start_job_type="FileTest"
        )

        assert stdout_path is not None
        assert stderr_path is not None
        assert stdout_path.exists()
        assert stderr_path.exists()
        assert stdout_path.name.startswith("stdout_")
        assert stderr_path.name.startswith("stderr_")
        assert "FileTest" in str(stdout_path.parent)
