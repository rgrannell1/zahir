from collections.abc import Callable
import inspect
from typing import Any, get_type_hints

from zahir.base_types import Job


def job(fn: Callable[..., Any]):
    """Construct a Job class from a run function.

    Allow other parameters to be passed as parameters to the
    decorator.
    """

    class_name = getattr(fn, "__name__", fn.__class__.__name__)
    mod = getattr(fn, "__module__", fn.__class__.__module__)
    qual = getattr(fn, "__qualname__", None)
    if isinstance(qual, str):
        qual = qual.rsplit(".", 1)[0]
    else:
        qual = class_name
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
