"""Capture worker stdout/stderr into shared log files with PID attribution.

Each worker process calls setup_output_logging() to redirect its fd 1 and fd 2
(via os.dup2) into stdout.log / stderr.log.  A PIDPrependingWriter replaces
sys.stdout and sys.stderr so every line is prefixed with [PID …].
"""

from datetime import UTC, datetime
from io import TextIOWrapper
import os
import pathlib
import sys


class PIDPrependingWriter:
    """Wraps a file object to prepend ``[PID <pid>]`` to each line written."""

    def __init__(self, file_obj: TextIOWrapper, process_id: int) -> None:
        self.file_obj = file_obj
        self.process_id = process_id
        self.buffer = ""

    def write(self, text: str) -> int:
        """Buffer text and emit complete lines with PID prefix."""
        if not text:
            return 0

        self.buffer += text
        lines = self.buffer.split("\n")

        # All but the last element are complete lines
        complete_lines = lines[:-1]
        self.buffer = lines[-1]

        for line in complete_lines:
            self.file_obj.write(f"[PID {self.process_id}] {line}\n")

        # Line-buffered file flushes on newlines already; explicit flush
        # only needed when we actually wrote something.
        if complete_lines:
            self.file_obj.flush()

        return len(text)

    def flush(self) -> None:
        """Flush any remaining buffer and the underlying file."""
        if self.buffer:
            self.file_obj.write(f"[PID {self.process_id}] {self.buffer}\n")
            self.buffer = ""
        self.file_obj.flush()
        fsync_file(self.file_obj)


def fsync_file(file_obj: TextIOWrapper) -> None:
    """Sync file to disk so content survives abrupt process termination."""
    try:
        os.fsync(file_obj.fileno())
    except (AttributeError, OSError):
        pass


def create_workflow_log_dir(
    log_output_dir: str,
    start_job_type: str | None = None,
) -> pathlib.Path:
    """Create a single log subdirectory for an entire workflow run.

    Called once in the overseer before spawning workers.  All workers
    then append to the same stdout.log / stderr.log inside this directory.
    """
    job_type_part = start_job_type or ""
    iso_timestamp = datetime.now(UTC).isoformat().replace(":", "-")
    log_subdir_name = f"{job_type_part}-{iso_timestamp}" if job_type_part else iso_timestamp

    log_path = pathlib.Path(log_output_dir) / log_subdir_name
    log_path.mkdir(parents=True, exist_ok=True)

    # Create the log files so symlinks are valid immediately
    stdout_log = log_path / "stdout.log"
    stderr_log = log_path / "stderr.log"
    stdout_log.touch()
    stderr_log.touch()

    # Symlink latest.stdout / latest.stderr in the root log dir for easy access.
    # Targets must be relative to the symlink's parent directory.
    log_root = pathlib.Path(log_output_dir)
    for name, log_file in (("latest.stdout", "stdout.log"), ("latest.stderr", "stderr.log")):
        link = log_root / name
        relative_target = pathlib.Path(log_subdir_name) / log_file
        link.unlink(missing_ok=True)
        link.symlink_to(relative_target)

    return log_path


def setup_output_logging(
    log_dir: str | None = None,
) -> tuple[pathlib.Path | None, pathlib.Path | None]:
    """Redirect this process's stdout/stderr into shared log files.

    All workers append to the same stdout.log and stderr.log; each line
    is prefixed with the PID so output can be attributed.

    The log_dir must already exist (created by create_workflow_log_dir).
    """
    if log_dir is None:
        return None, None

    log_path = pathlib.Path(log_dir)
    current_pid = os.getpid()
    stdout_log_path = log_path / "stdout.log"
    stderr_log_path = log_path / "stderr.log"

    # Files must stay open for process lifetime — no context manager
    stdout_file = stdout_log_path.open("a", encoding="utf-8", buffering=1)  # noqa: SIM115
    stderr_file = stderr_log_path.open("a", encoding="utf-8", buffering=1)  # noqa: SIM115

    # Replace fd 1 and 2 at the OS level so even os.write(1, …) goes here
    os.dup2(stdout_file.fileno(), 1)
    os.dup2(stderr_file.fileno(), 2)

    sys.stdout = PIDPrependingWriter(stdout_file, current_pid)
    sys.stderr = PIDPrependingWriter(stderr_file, current_pid)

    return stdout_log_path, stderr_log_path
