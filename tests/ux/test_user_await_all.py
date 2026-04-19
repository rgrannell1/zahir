from tertius import EEmit, ESleep

from zahir.core.effects import EAwaitAll
from zahir.core.evaluate import JobContext, evaluate
from zahir.core.exceptions import JobError


def double(ctx: JobContext, value: int):
    return value * 2
    yield


def slow_double(ctx: JobContext, value: int):
    yield ESleep(ms=200)
    return value * 2
    yield


def fan_out(ctx: JobContext):
    results = yield EAwaitAll(
        [
            ctx.scope.double(1),
            ctx.scope.double(2),
            ctx.scope.double(3),
        ]
    )
    yield EEmit(results)


def failing_job(ctx: JobContext):
    raise ValueError("boom")
    yield


def fan_out_with_failure(ctx: JobContext):
    try:
        yield EAwaitAll(
            [
                ctx.scope.double(1),
                ctx.scope.failing_job(),
                ctx.scope.double(3),
            ]
        )
    except JobError as err:
        yield EEmit({"error": str(err.cause)})


def fan_out_mixed(ctx: JobContext):
    results = yield EAwaitAll(
        [
            ctx.scope.slow_double(1),
            ctx.scope.double(2),
        ]
    )
    yield EEmit(results)


def test_await_all_returns_results_in_input_order():
    """Proves EAwaitAll returns results ordered by dispatch position, not arrival order."""

    scope = {"fan_out": fan_out, "double": double}
    events = list(evaluate("fan_out", (), scope, n_workers=4))

    assert events == [[2, 4, 6]]


def test_await_all_preserves_order_when_completions_arrive_out_of_order():
    """Proves EAwaitAll result ordering matches dispatch order even when a slow job finishes last."""

    scope = {
        "fan_out_mixed": fan_out_mixed,
        "slow_double": slow_double,
        "double": double,
    }
    events = list(evaluate("fan_out_mixed", (), scope, n_workers=4))

    assert events == [[2, 4]]


def test_await_all_raises_job_error_on_failure():
    """Proves EAwaitAll raises JobError if any child job crashes."""

    scope = {
        "fan_out_with_failure": fan_out_with_failure,
        "double": double,
        "failing_job": failing_job,
    }
    events = list(evaluate("fan_out_with_failure", (), scope, n_workers=4))

    assert events == [{"error": "boom"}]
