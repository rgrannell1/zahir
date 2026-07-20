"""Wraps the job scope so attribute access returns typed EAwait factories."""

import functools
import inspect
from collections.abc import Callable
from typing import Any

from zahir.core.commons.zahir_types import JobSpec
from zahir.core.effects import EAwait


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
            raise AttributeError(f"no job named {name!r} in scope") from None

        params = list(inspect.signature(fn).parameters.values())[1:]  # strip ctx
        bound_sig = inspect.Signature(params)
        timeout_param = inspect.Parameter(
            "timeout_ms", inspect.Parameter.KEYWORD_ONLY, default=None
        )

        @functools.wraps(fn)
        def dispatch(*args: Any, timeout_ms: int | None = None, **kwargs: Any) -> EAwait:
            bound = bound_sig.bind(*args, **kwargs)
            bound.apply_defaults()
            job_spec = JobSpec(
                fn_name=name, args=bound.args, kwargs=bound.kwargs, timeout_ms=timeout_ms
            )
            return EAwait(jobs=[job_spec], scalar=True)

        dispatch.__signature__ = inspect.Signature([*params, timeout_param])  # type: ignore[attr-defined]
        self.__dict__[name] = dispatch
        return dispatch
