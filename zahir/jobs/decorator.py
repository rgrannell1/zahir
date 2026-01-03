from collections.abc import Callable
import inspect
from typing import Any, get_type_hints

from zahir.base_types import Job


def job(
    fn: Callable[..., Any] | None = None,
    *,
    recovery: Callable[..., Any] | None = None,
):
    """Construct a Job class from a run function.

    @param fn: The function to wrap as a Job's run method
    @param recovery: Optional recovery function to use when the job fails

    Examples:
        # Simple job without recovery
        @job
        def MyJob(context, input, dependencies):
            yield JobOutputEvent({"result": "done"})

        # Job with recovery function
        def my_recovery(context, input, dependencies, err):
            yield JobOutputEvent({"recovered": True})

        @job(recovery=my_recovery)
        def MyJob(context, input, dependencies):
            yield JobOutputEvent({"result": "done"})
    """

    def decorator(func: Callable[..., Any]) -> type[Job]:
        class_name = getattr(func, "__name__", func.__class__.__name__)
        mod = getattr(func, "__module__", func.__class__.__module__)
        qual = getattr(func, "__qualname__", None)
        qual = qual.rsplit(".", 1)[0] if isinstance(qual, str) else class_name
        class_qualname = f"{mod}.{qual}.{class_name}" if qual else f"{mod}.{class_name}"

        # Wrapper that adds cls parameter for the classmethod
        def run_wrapper(_, context, input, dependencies):
            return func(context, input, dependencies)

        ns: dict[str, Any] = {}

        # the main requirement; define `run`
        ns["run"] = classmethod(run_wrapper)
        ns["job_options"] = None

        # A recovery method, if the user wants one too.
        if recovery is not None:

            def recovery_wrapper(_, context, input, dependencies, err):
                return recovery(context, input, dependencies, err)

            ns["recover"] = classmethod(recovery_wrapper)

        ns["__module__"] = mod
        ns["__qualname__"] = class_qualname
        ns["__doc__"] = func.__doc__

        ns["__run_function__"] = func
        ns["__signature__"] = inspect.signature(func)

        try:
            ns["__type_hints__"] = get_type_hints(func, include_extras=True)
        except Exception:
            ns["__type_hints__"] = {}

        return type(class_name, (Job,), ns)

    # Handle both @job and @job(recovery=...)
    if fn is None:
        # Called with arguments: @job(recovery=...)
        return decorator
    # Called without arguments: @job
    return decorator(fn)
