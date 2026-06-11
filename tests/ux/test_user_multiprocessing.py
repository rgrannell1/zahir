from tertius import EEmit

from tests.shared import user_events
from zahir.core.combinators import wrap
from zahir.core.evaluate import JobContext, evaluate, setup


def identity(effect):
    yield


def inc(ctx: JobContext, num: int) -> int:
    yield from ()
    return num + 1  # noqa: B901


def chain_and_sum(ctx: JobContext):
    """Await inc ten times in sequence, accumulating the running total."""
    num = 0
    total = 0
    for _ in range(10):
        num = yield ctx.scope.inc(num)
        total += num
    yield EEmit(total)


def test_chained_awaits_across_ten_workers():
    """Proves a chain of ten sequential awaits completes correctly across ten worker processes."""

    scope = {"chain_and_sum": chain_and_sum, "inc": inc}
    events = user_events(evaluate(setup(n_workers=10), "chain_and_sum", (), scope))

    assert events == [55]


def test_chained_awaits_with_identity_telemetry():
    """Proves an identity handler_wrapper does not alter behaviour across ten worker processes."""

    scope = {"chain_and_sum": chain_and_sum, "inc": inc}
    wrapper = wrap(identity)
    result = evaluate(
        setup(n_workers=10),
        "chain_and_sum",
        (),
        scope,
        handler_wrappers=(wrapper,),
    )
    events = user_events(result)

    assert events == [55]
