"""JobArguments timeout fields must be numeric so the state machine can compare them safely."""

import pytest

from zahir.dependencies.group import DependencyGroup
from zahir.types.job import JobArguments


def test_job_timeout_dict_raises_helpful_typeerror() -> None:
    with pytest.raises(TypeError, match="job_timeout.*dict"):
        JobArguments(
            dependencies=DependencyGroup({}),
            args={},
            job_id="j1",
            job_timeout={"units": "seconds"},
        )


def test_recover_timeout_non_numeric_raises() -> None:
    with pytest.raises(TypeError, match="recover_timeout"):
        JobArguments(
            dependencies=DependencyGroup({}),
            args={},
            job_id="j1",
            recover_timeout=["1"],
        )


def test_bool_timeout_rejected() -> None:
    with pytest.raises(TypeError, match="not bool"):
        JobArguments(
            dependencies=DependencyGroup({}),
            args={},
            job_id="j1",
            job_timeout=True,
        )
