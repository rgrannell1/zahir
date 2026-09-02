"""Higher-order functions for composing operation interpreters."""

import inspect
from collections.abc import Generator, Mapping, Sequence
from collections.abc import Set as AbstractSet
from functools import partial, reduce
from typing import Any

from zahir.core.commons.zahir_types import HandlerCallable


def apply_wrapper(interpreter: Any, wrapper: Any) -> Any:
    """Apply one wrapper to an operation interpreter."""

    return wrapper(interpreter)


def build_handler_map(
    bindings: Mapping[str, HandlerCallable],
    wrappers: Sequence[Any] = (),
    skip: AbstractSet[str] = frozenset(),
) -> dict[str, HandlerCallable]:
    """Build a tag-to-interpreter map and apply each wrapper in order.

    Handlers whose tag is in skip are left unwrapped. With no wrappers, handlers
    are returned as-is.
    """

    return {
        tag: handler if tag in skip else reduce(apply_wrapper, wrappers, handler)
        for tag, handler in bindings.items()
    }


def merge_handlers(*handler_maps: Mapping[str, HandlerCallable]) -> dict[str, HandlerCallable]:
    """Merge handler maps left-to-right; later maps override earlier ones on tag collision."""

    merged: dict[str, HandlerCallable] = {}
    for handler_map in handler_maps:
        merged.update(handler_map)
    return merged


def lift(fn, *args) -> Generator[Any, Any, Any]:
    """Lift a plain call into a no-effect generator.

    Used with partial to turn plain functions into operation interpreters or
    dependency conditions without each site making a yield-less generator.
    """

    yield from ()
    return fn(*args)


def _drive_setup(gen) -> Generator[Any, Any, None]:
    """Propagate fn setup yields to the caller until the seam (bare yield -> None)."""

    inner = next(gen)
    while inner is not None:
        sent = yield inner
        inner = gen.send(sent)


def _drive_teardown(gen, exc_caught, result) -> Generator[Any, Any, None]:
    """Drive fn teardown after the seam, propagating yields and absorbing StopIteration.

    If exc_caught is set, throws it into gen so teardown can observe the error.
    If the exception propagates out of gen (i.e. fn did not catch it), swallow it -
    the caller will re-raise exc_caught itself.
    """

    try:
        yielded = gen.throw(exc_caught) if exc_caught is not None else gen.send(result)
        while True:
            sent = yield yielded
            yielded = gen.send(sent)
    except StopIteration:
        pass
    except Exception:  # noqa: BLE001
        pass


def _wrap_call(wrapper_fn, interpreter, operation) -> Generator[Any, Any, Any]:
    """Run a two-phase wrapper around one interpreter call.

    wrapper_fn(operation) has two phases separated by a bare yield that yields None:
    - setup: yields operations propagated to the caller before the interpreter runs
    - teardown: receives the result via send, or the exception via throw
                if the interpreter raised. The wrapper may catch the exception to emit
                error telemetry. Otherwise, the exception propagates normally.
    """

    gen = wrapper_fn(operation)
    yield from _drive_setup(gen)

    exc_caught = None
    result = None
    try:
        result = interpreter(operation)
        if inspect.isgenerator(result):
            result = yield from result
    except Exception as exc:  # noqa: BLE001
        exc_caught = exc

    yield from _drive_teardown(gen, exc_caught, result)

    if exc_caught is not None:
        raise exc_caught
    return result


def _wrap_handler(wrapper_fn, interpreter):
    """Bind a wrapper to one operation interpreter."""

    return partial(_wrap_call, wrapper_fn, interpreter)


def wrap(wrapper_fn):
    """Apply a two-phase wrapper around an operation interpreter."""

    return partial(_wrap_handler, wrapper_fn)
