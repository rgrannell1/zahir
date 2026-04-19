import functools
import inspect
from collections.abc import Callable
from typing import Any

from effects import EAwait


class ScopeProxy:
    """Wraps the job scope so attribute access returns typed EAwait factories.

    ctx.scope.my_job(arg1, arg2) returns EAwait(fn_name="my_job", args=(arg1, arg2))
    rather than calling the function. The wrapper has the original signature minus
    the leading ctx parameter, so IDEs and type checkers see the correct types.
    """

    def __init__(self, scope: dict[str, Callable[..., Any]]) -> None:
        self._scope = scope

    def __getattr__(self, name: str) -> Callable[..., EAwait]:
        try:
            fn = self._scope[name]
        except KeyError:
            raise AttributeError(f"no job named {name!r} in scope")

        params = list(inspect.signature(fn).parameters.values())[1:]  # strip ctx

        @functools.wraps(fn)
        def dispatch(*args: Any, timeout_ms: int | None = None, **kwargs: Any) -> EAwait:
            bound = inspect.Signature(params).bind(*args, **kwargs)
            bound.apply_defaults()
            return EAwait(fn_name=name, args=bound.args, timeout_ms=timeout_ms)

        dispatch.__signature__ = inspect.Signature(  # type: ignore[attr-defined]
            [*params, inspect.Parameter("timeout_ms", inspect.Parameter.KEYWORD_ONLY, default=None)]
        )
        return dispatch
