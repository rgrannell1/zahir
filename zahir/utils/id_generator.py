"""Typed wrapper for ID generation."""

import secrets
import string
import uuid

from coolname import generate_slug  # type: ignore[import-untyped]


def generate_id() -> str:
    random_suffix = "".join(secrets.choice(string.ascii_lowercase) for _ in range(10))
    return generate_slug(2) + "-" + random_suffix


def generate_job_id(job_type: str) -> str:
    return f"{job_type}/{generate_id()}"


def generate_trace_id() -> str:
    """Generate a 32-character hex trace ID."""
    return format(uuid.uuid4().int & (1 << 128) - 1, "032x")


def generate_span_id() -> str:
    """Generate a 16-character hex span ID."""
    return format(uuid.uuid4().int & (1 << 64) - 1, "016x")
