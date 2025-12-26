from collections.abc import Callable
import inspect
from typing import Any, get_type_hints

from zahir.types import Job


def job(fn: Callable[..., Any]):
    """Construct a Job class from a run function."""

    class_name = fn.__name__
    mod = fn.__module__
    qual = fn.__qualname__.rsplit(".", 1)[0]
    class_qualname = f"{mod}.{qual}.{class_name}" if qual else f"{mod}.{class_name}"

    ns: dict[str, Any] = {}

    # the main requirement; define `run`
    ns["run"] = classmethod(fn)

    ns["__module__"] = mod
    ns["__qualname__"] = class_qualname
    ns["__doc__"] = fn.__doc__

    ns["__run_function__"] = fn
    ns["__signature__"] = inspect.signature(fn)

    try:
        ns["__type_hints__"] = get_type_hints(fn, include_extras=True)
    except Exception:
        ns["__type_hints__"] = {}

    ConstructedClass = type(class_name, (Job,), ns)
    return ConstructedClass
