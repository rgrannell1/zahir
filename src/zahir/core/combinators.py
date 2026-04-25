# Higher-order functions for composing effect handlers
from collections.abc import Generator
from functools import partial
from typing import Any


def _drive_setup(gen) -> Generator[Any, Any, None]:
    """Propagate fn setup yields to the caller until the seam (bare yield → None)."""

    inner = next(gen)
    while inner is not None:
        sent = yield inner
        inner = gen.send(sent)


def _drive_teardown(gen, exc_caught, result) -> Generator[Any, Any, None]:
    """Drive fn teardown after the seam, propagating yields and absorbing StopIteration.

    If exc_caught is set, throws it into gen so teardown can observe the error.
    If the exception propagates out of gen (i.e. fn did not catch it), swallow it —
    the caller will re-raise exc_caught itself.
    """

    try:
        yielded = gen.throw(exc_caught) if exc_caught is not None else gen.send(result)
        while True:
            sent = yield yielded
            yielded = gen.send(sent)
    except StopIteration:
        pass
    except Exception:
        pass


def _wrap_call(fn, handler, effect) -> Generator[Any, Any, Any]:
    """Generator body: run fn's two-phase setup/teardown around one handler call.

    fn(effect) is a generator with two phases separated by a bare yield (yields None):
    - setup:    yields effects (e.g. EEmit) propagated to the caller before the handler runs
    - teardown: runs after the handler; receives the result via send, or the exception via throw
                if the handler raised. fn may optionally catch the thrown exception to emit
                error telemetry — if it does not, the exception propagates normally.
    """

    gen = fn(effect)
    yield from _drive_setup(gen)

    exc_caught = None
    result = None
    try:
        result = yield from handler(effect)
    except Exception as exc:
        exc_caught = exc

    yield from _drive_teardown(gen, exc_caught, result)

    if exc_caught is not None:
        raise exc_caught
    return result


def _wrap_handler(fn, handler):
    """Bind fn to a specific handler, producing an effect-level callable."""

    return partial(_wrap_call, fn, handler)


def wrap(fn):
    """Combinator that applies fn around each job-effect handler call.

    fn(effect) is a generator with two phases separated by a bare yield (yields None):
    - setup:    yields effects (e.g. EEmit) propagated to the caller before the handler runs
    - teardown: runs after the handler; receives the result via send, or the exception via throw
                if the handler raised. fn may optionally catch the thrown exception to emit
                error telemetry — if it does not, the exception propagates normally.

    Example:
        def my_wrapper(effect):
            yield EEmit({"event": "start", "tag": effect.tag})  # setup
            try:
                result = yield                                    # seam
                yield EEmit({"event": "end", "error": None})    # success teardown
            except Exception as exc:
                yield EEmit({"event": "end", "error": str(exc)}) # error teardown
    """

    return partial(_wrap_handler, fn)
