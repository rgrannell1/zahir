from collections.abc import Generator
from typing import Any


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
    def wrapper(handler):
        def wrapped(effect) -> Generator[Any, Any, Any]:
            gen = fn(effect)
            # propagate setup yields until the seam (bare yield produces None)
            inner = next(gen)
            while inner is not None:
                sent = yield inner
                inner = gen.send(sent)

            # run the handler, capturing any exception
            exc_caught = None
            try:
                result = yield from handler(effect)
            except Exception as exc:
                exc_caught = exc

            # teardown — throw exception into fn if one occurred, otherwise send result
            try:
                if exc_caught is not None:
                    yielded = gen.throw(exc_caught)
                else:
                    yielded = gen.send(result)
                while True:
                    sent = yield yielded
                    yielded = gen.send(sent)
            except StopIteration:
                pass
            except Exception:
                pass  # fn did not catch the thrown exception — that's fine

            if exc_caught is not None:
                raise exc_caught
            return result

        return wrapped

    return wrapper
