from tertius import EEmit

from zahir.core.evaluate import evaluate, JobContext
from zahir.core.scope_proxy import ScopeProxy


def child_job(ctx: JobContext, value: int):
    return value * 2
    yield


def parent_job(ctx: JobContext):
    result = yield ctx.scope.child_job(21)
    yield EEmit({"result": result})


def test_scope_proxy_returns_eawait_with_correct_args():
    """Proves ScopeProxy.__getattr__ returns an EAwait with fn_name and args set."""

    from zahir.core.effects import EAwait

    scope = {"child_job": child_job}
    proxy = ScopeProxy(scope)

    effect = proxy.child_job(99)
    assert effect == EAwait(fn_name="child_job", args=(99,))


def test_scope_proxy_strips_ctx_from_signature():
    """Proves ScopeProxy exposes the job signature without the leading ctx parameter."""

    import inspect

    scope = {"child_job": child_job}
    proxy = ScopeProxy(scope)

    params = list(inspect.signature(proxy.child_job).parameters)
    assert "ctx" not in params
    assert "value" in params


def test_scope_proxy_raises_attribute_error_for_unknown_job():
    """Proves ScopeProxy raises AttributeError when the job name is not in scope."""

    import pytest

    proxy = ScopeProxy({})
    with pytest.raises(AttributeError, match="no job named"):
        proxy.missing_job


def test_scope_proxy_dispatches_typed_await():
    """Proves ctx.scope.<fn>(args) produces a correctly dispatched EAwait end-to-end."""

    scope = {"parent_job": parent_job, "child_job": child_job}
    events = list(evaluate("parent_job", (), scope, n_workers=2))

    assert events == [{"result": 42}]
