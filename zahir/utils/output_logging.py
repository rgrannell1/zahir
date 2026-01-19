"""Utilities for capturing and logging process stdout/stderr with PID prepending."""

from datetime import UTC, datetime
import os
import pathlib
import sys


class PIDPrependingWriter:
    """Wraps a file object to prepend the PID to each line written."""

    def __init__(self, file_obj, process_id: int):
        self.file_obj = file_obj
        self.process_id = process_id
        self.buffer = ""

    def write(self, text: str) -> int:
        if not text:
            return 0

        self.buffer += text
        lines = self.buffer.split("\n")

        # All but the last element are complete lines
        complete_lines = lines[:-1]
        self.buffer = lines[-1]

        for line in complete_lines:
            self.file_obj.write(f"[PID {self.process_id}] {line}\n")
            self.file_obj.flush()

        return len(text)

    def flush(self) -> None:
        """Flush any remaining buffer and the underlying file."""
        if self.buffer:
            self.file_obj.write(f"[PID {self.process_id}] {self.buffer}\n")
            self.buffer = ""
        self.file_obj.flush()


def setup_output_logging(
    log_output_dir: str | None = None,
    start_job_type: str | None = None,
) -> tuple[pathlib.Path | None, pathlib.Path | None]:
    if log_output_dir is None:
        return None, None

    current_pid = os.getpid()
    job_type_part = start_job_type or ""
    iso_timestamp = datetime.now(UTC).isoformat().replace(":", "-")
    log_subdir_name = f"{job_type_part}-{iso_timestamp}" if job_type_part else iso_timestamp

    log_path = pathlib.Path(log_output_dir) / log_subdir_name
    log_path.mkdir(parents=True, exist_ok=True)

    stdout_log_path = log_path / f"stdout_{current_pid}.log"
    stderr_log_path = log_path / f"stderr_{current_pid}.log"

    # Files must stay open for process lifetime, so context manager is not appropriate
    stdout_file = pathlib.Path(stdout_log_path).open("a", encoding="utf-8", buffering=1)  # noqa: SIM115
    stderr_file = pathlib.Path(stderr_log_path).open("a", encoding="utf-8", buffering=1)  # noqa: SIM115

    sys.stdout = PIDPrependingWriter(stdout_file, current_pid)
    sys.stderr = PIDPrependingWriter(stderr_file, current_pid)

    return stdout_log_path, stderr_log_path


def get_log_directory_path(
    base_dir: str,
    start_job_type: str | None = None,
) -> pathlib.Path:
    job_type_part = start_job_type or ""
    iso_timestamp = datetime.now(UTC).isoformat().replace(":", "-")
    log_subdir_name = f"{job_type_part}-{iso_timestamp}" if job_type_part else iso_timestamp

    return pathlib.Path(base_dir) / log_subdir_name
