from __future__ import annotations

from typing import TypeVar

from zahir.base_types import JobSpec, Run

ArgsType = TypeVar("ArgsType")
OutputType = TypeVar("OutputType")


def spec[ArgsType, OutputType](**kwargs):
    """Construct a JobSpec from a run function, and optionally other jobspec parameters"""

    def decorator(run: Run[ArgsType, OutputType]) -> JobSpec[ArgsType, OutputType]:
        # At runtime, run is a function which has __name__, but the type system
        # sees it as Callable which doesn't have this attribute. We use getattr
        # to safely access it.
        run_name = getattr(run, "__name__", "unknown")
        job_spec = JobSpec[ArgsType, OutputType](type=run_name, run=run, **kwargs)
        # Add __name__ attribute to JobSpec for backwards compatibility with tests
        job_spec.__name__ = run_name  # type: ignore[attr-defined]
        return job_spec

    return decorator
