from __future__ import annotations

from typing import TypeVar

from zahir.base_types import JobSpec, Run

ArgsType = TypeVar("ArgsType")
OutputType = TypeVar("OutputType")


def spec[JobSpecArgs, ArgsType, OutputType](**kwargs):
    """Construct a JobSpec from a run function, and optionally other jobspec parameters"""

    def decorator(run: Run[JobSpecArgs, ArgsType, OutputType]) -> JobSpec[JobSpecArgs, ArgsType, OutputType]:
        job_spec = JobSpec[JobSpecArgs, ArgsType, OutputType](type=run.__name__, run=run, **kwargs)
        # Add __name__ attribute to JobSpec for backwards compatibility with tests
        job_spec.__name__ = run.__name__
        return job_spec

    return decorator
