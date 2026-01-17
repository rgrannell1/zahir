from __future__ import annotations

from collections.abc import Callable, Iterator
from typing import Any, TypeVar, overload

from zahir.base_types import Job, JobSpec, Run

ArgsType = TypeVar("ArgsType")
OutputType = TypeVar("OutputType")

# You can tighten these if you have real types
Context = Any
Dependencies = Any

RunFn = Callable[[Context, ArgsType, Dependencies], Iterator[OutputType]]
RecoveryFn = Callable[[Context, ArgsType, Dependencies, Exception], Iterator[OutputType]]


@overload
def job() -> Callable[[RunFn[ArgsType, OutputType]], type[Job[ArgsType, OutputType]]]: ...
@overload
def job[ArgsType, OutputType](
    *,
    recovery: RecoveryFn[ArgsType, OutputType],
) -> Callable[[RunFn[ArgsType, OutputType]], type[Job[ArgsType, OutputType]]]: ...


def job[ArgsType, OutputType](
    *, recovery: RecoveryFn[ArgsType, OutputType] | None = None
) -> Callable[[RunFn[ArgsType, OutputType]], type[Job[ArgsType, OutputType]]]:
    """Construct a Job class from a run function.

    Forced usage:
        @job()
        def MyJob(...): ...

        @job(recovery=my_recovery)
        def MyJob(...): ...
    """

    def decorator(func: RunFn[ArgsType, OutputType]) -> type[Job[ArgsType, OutputType]]:
        class_name = getattr(func, "__name__", func.__class__.__name__)
        mod = getattr(func, "__module__", func.__class__.__module__)
        qual = getattr(func, "__qualname__", None)
        qual = qual.rsplit(".", 1)[0] if isinstance(qual, str) else class_name
        class_qualname = class_name

        # Wrapper that adds cls parameter for the classmethod
        def run_wrapper(
            cls: type[Job[ArgsType, OutputType]],
            context: Context,
            input: ArgsType,
            dependencies: Dependencies,
        ) -> Iterator[OutputType]:
            return func(context, input, dependencies)

        ns: dict[str, Any] = {}

        # the main requirement; define `run`
        ns["run"] = classmethod(run_wrapper)
        ns["job_options"] = None

        # A recovery method, if the user wants one too.
        if recovery is not None:

            def recovery_wrapper(
                cls: type[Job[ArgsType, OutputType]],
                context: Context,
                input: ArgsType,
                dependencies: Dependencies,
                err: Exception,
            ) -> Iterator[OutputType]:
                assert recovery is not None  # keep our type-checker happy.
                return recovery(context, input, dependencies, err)

            ns["recover"] = classmethod(recovery_wrapper)

        ns["__module__"] = mod
        ns["__qualname__"] = class_qualname
        ns["__doc__"] = func.__doc__

        created: type[Job[ArgsType, OutputType]] = type(class_name, (Job,), ns)  # type: ignore[assignment]
        return created

    return decorator


def spec[JobSpecArgs, ArgsType, OutputType](**kwargs):
    """Construct a JobSpec from a run function, and optionally other jobspec parameters"""

    def decorator(run: Run[JobSpecArgs, ArgsType, OutputType]) -> JobSpec[JobSpecArgs, ArgsType, OutputType]:
        job_spec = JobSpec[JobSpecArgs, ArgsType, OutputType](type=run.__name__, run=run, **kwargs)
        # Add __name__ attribute to JobSpec for backwards compatibility with tests
        job_spec.__name__ = run.__name__
        return job_spec

    return decorator
