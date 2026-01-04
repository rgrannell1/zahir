from collections.abc import Mapping
from typing import Any, TypedDict


class JobOptionsData(TypedDict, total=False):
    """Serialized structure for job options."""

    # Upper-limit on how long the job should run for
    job_timeout: float | None
    # Upper-limit on how long the recovery should run for
    recover_timeout: float | None


class SerialisedJob(TypedDict):
    """Serialized representation of a Job"""

    type: str

    # Unique job identifier
    job_id: str

    # Optional parent job identifier
    parent_id: str | None

    # The input parameters to the job. Must be JSON-serialisable
    input: Mapping[str, Any]

    # The serialised dependencies for the job
    dependencies: Mapping[str, Any]

    # The serialised options for the job
    options: JobOptionsData | None
