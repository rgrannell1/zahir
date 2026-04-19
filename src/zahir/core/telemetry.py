from collections.abc import Generator
from typing import Any


def wrap(fn):
    """Combinator that applies f around each job-effect handler call.

    f(effect) is a generator with two phases separated by a yield:
    - setup: runs synchronously before the handler (capture start time, open span, etc.)
    - teardown: runs after send(result), and may itself yield effects (e.g. EEmit)

    Example:
        def my_wrapper(effect):
            started_at = time.monotonic()
            result = yield
            yield EEmit({"tag": effect.tag, "duration_ms": (time.monotonic() - started_at) * 1000})
    """
    def wrapper(handler):
        def wrapped(effect) -> Generator[Any, Any, Any]:
            gen = fn(effect)
            next(gen)                              # synchronous setup, advances to seam
            result = yield from handler(effect)
            try:
                yielded = gen.send(result)         # kick off teardown
                while True:                        # propagate any teardown yields
                    sent = yield yielded
                    yielded = gen.send(sent)
            except StopIteration:
                pass
            return result

        return wrapped

    return wrapper
