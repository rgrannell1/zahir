"""Typed wrapper for ID generation."""

import secrets
import string

from coolname import generate_slug  # type: ignore[import-untyped]


def generate_id() -> str:
    random_suffix = "".join(secrets.choice(string.ascii_lowercase) for _ in range(10))
    return generate_slug(2) + "-" + random_suffix


def generate_job_id(job_type: str) -> str:
    return f"{job_type}/{generate_id()}"
