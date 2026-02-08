"""Serialised (TypedDict) forms for job and transform data.

These are the on-disk / over-the-wire representations used by
save/load round-trips. They carry no runtime Zahir dependencies.
"""

from collections.abc import Mapping
from typing import Any, TypedDict


class SerialisedTransformSpec(TypedDict):
    """Serialised form of a transform specification."""

    type: str
    args: Mapping[str, Any]


class SerialisedJobData(TypedDict):
    type: str
    transforms: list[SerialisedTransformSpec]


class SerialisedJobInstance[ArgsType](TypedDict, total=False):
    """A stored request for a job execution. Includes which
    `type`, arguments and dependencies, and timeouts."""

    type: str
    transforms: list[SerialisedTransformSpec]
    job_id: str
    parent_id: str | None
    args: ArgsType
    dependencies: Mapping[str, Any]
    job_timeout: float | None
    recover_timeout: float | None
    priority: int
    once: bool
