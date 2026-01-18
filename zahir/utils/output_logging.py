"""Utilities for capturing and logging process stdout/stderr with PID prepending."""

import os
import pathlib
from datetime import datetime, timezone
from typing import Optional
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
    log_output_dir: Optional[str] = None,
    start_job_type: Optional[str] = None,
) -> tuple[Optional[pathlib.Path], Optional[pathlib.Path]]:
    if log_output_dir is None:
        return None, None

    current_pid = os.getpid()
    job_type_part = start_job_type or ""
    iso_timestamp = datetime.now(timezone.utc).isoformat().replace(":", "-")
    log_subdir_name = f"{job_type_part}-{iso_timestamp}" if job_type_part else iso_timestamp

    log_path = pathlib.Path(log_output_dir) / log_subdir_name
    log_path.mkdir(parents=True, exist_ok=True)

    stdout_log_path = log_path / f"stdout_{current_pid}.log"
    stderr_log_path = log_path / f"stderr_{current_pid}.log"

    stdout_file = open(stdout_log_path, "a", buffering=1)
    stderr_file = open(stderr_log_path, "a", buffering=1)

    sys.stdout = PIDPrependingWriter(stdout_file, current_pid)
    sys.stderr = PIDPrependingWriter(stderr_file, current_pid)

    return stdout_log_path, stderr_log_path


def get_log_directory_path(
    base_dir: str,
    start_job_type: Optional[str] = None,
) -> pathlib.Path:
    job_type_part = start_job_type or ""
    iso_timestamp = datetime.now(timezone.utc).isoformat().replace(":", "-")
    log_subdir_name = f"{job_type_part}-{iso_timestamp}" if job_type_part else iso_timestamp

    return pathlib.Path(base_dir) / log_subdir_name
