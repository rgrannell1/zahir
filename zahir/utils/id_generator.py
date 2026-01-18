"""Typed wrapper for ID generation."""

import secrets
import string

from coolname import generate_slug  # type: ignore[import-untyped]


def generate_id() -> str:
    """Generate a unique slug-style identifier with a random suffix.

    @return: A hyphenated slug string with a 10-letter random suffix
            (e.g., "purple-elephant-abcdefghij")
    """
    random_suffix = "".join(secrets.choice(string.ascii_lowercase) for _ in range(10))
    return generate_slug(2) + "-" + random_suffix


def generate_job_id(job_type: str) -> str:
    """Generate a unique job identifier with the job type as prefix.

    @param job_type: The type/name of the job (e.g., "ProcessData", "SendEmail")
    @return: A job ID in the format "<job-type>/<coolname><random>"
            (e.g., "ProcessData/purple-elephant-a1b2c3")
    """
    random_suffix = "".join(secrets.choice(string.ascii_lowercase + string.digits) for _ in range(6))
    slug = generate_slug(2)
    return f"{job_type}/{slug}-{random_suffix}"


def job_type_from_id(job_id: str) -> str | None:
    """Extract the job type from a job ID.

    @param job_id: The job ID (e.g., "ProcessData/purple-elephant-a1b2c3")
    @return: The job type, or None if the ID doesn't contain a type prefix
    """
    if "/" in job_id:
        return job_id.split("/", 1)[0]
    return None
