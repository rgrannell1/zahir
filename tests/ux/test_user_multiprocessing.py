from tertius import EEmit

from zahir.core.evaluate import JobContext, evaluate
from zahir.core.combinators import wrap


def identity(effect):
    yield


def inc(ctx: JobContext, num: int) -> int:
    return num + 1
    yield


def chain_and_sum(ctx: JobContext):
    """Await inc ten times in sequence, accumulating the running total."""
    num = 0
    total = 0
    for _ in range(10):
        num = yield ctx.scope.inc(num)
        total += num
    yield EEmit(total)


class IdentityContext(JobContext):
    handler_wrappers = (wrap(identity),)


def test_chained_awaits_across_ten_workers():
    """Proves a chain of ten sequential awaits completes correctly across ten worker processes."""

    scope = {"chain_and_sum": chain_and_sum, "inc": inc}
    events = list(evaluate("chain_and_sum", (), scope, n_workers=10))

    assert events == [55]


def test_chained_awaits_with_identity_telemetry():
    """Proves an identity handler_wrapper does not alter behaviour across ten worker processes."""

    scope = {"chain_and_sum": chain_and_sum, "inc": inc}
    events = list(
        evaluate("chain_and_sum", (), scope, n_workers=10, context=IdentityContext)
    )

    assert events == [55]
