import functools
import inspect
from collections.abc import Callable
from typing import Any

from zahir.core.effects import EAwait
from zahir.core.zahir_types import JobSpec


class ScopeProxy:
    """Wraps the job scope so attribute access returns typed EAwait factories.

    ctx.scope.my_job(arg1, arg2) returns EAwait(jobs=[JobSpec("my_job", (arg1, arg2))], scalar=True)
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

        # the function signature, minus context
        params = list(inspect.signature(fn).parameters.values())[1:]  # strip ctx

        # build a wrapper that returns an EAwait with the correct fn_name and args, and a timeout_ms keyword argument
        @functools.wraps(fn)
        def dispatch(
            *args: Any, timeout_ms: int | None = None, **kwargs: Any
        ) -> EAwait:
            bound = inspect.Signature(params).bind(*args, **kwargs)
            bound.apply_defaults()

            return EAwait(jobs=[JobSpec(fn_name=name, args=bound.args, timeout_ms=timeout_ms)], scalar=True)

        dispatch.__signature__ = inspect.Signature(  # type: ignore[attr-defined]
            [
                *params,
                inspect.Parameter(
                    "timeout_ms", inspect.Parameter.KEYWORD_ONLY, default=None
                ),
            ]
        )
        return dispatch
