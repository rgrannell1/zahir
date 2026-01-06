from __future__ import annotations

from collections.abc import Callable, Iterator
from typing import Any, TypeVar, overload

from zahir.base_types import Job

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
def job(
    *,
    recovery: RecoveryFn[ArgsType, OutputType],
) -> Callable[[RunFn[ArgsType, OutputType]], type[Job[ArgsType, OutputType]]]: ...


def job(*, recovery: RecoveryFn[ArgsType, OutputType] | None = None):
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
        class_qualname = f"{mod}.{qual}.{class_name}" if qual else f"{mod}.{class_name}"

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
                return recovery(context, input, dependencies, err)

            ns["recover"] = classmethod(recovery_wrapper)

        ns["__module__"] = mod
        ns["__qualname__"] = class_qualname
        ns["__doc__"] = func.__doc__

        created: type[Job[ArgsType, OutputType]] = type(class_name, (Job,), ns)  # type: ignore[assignment]
        return created

    return decorator
