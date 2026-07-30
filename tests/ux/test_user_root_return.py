"""UX test: verifies that a root job's return value is surfaced by evaluate."""

from tertius import EEmit

from tests.shared import root_value, user_events
from zahir.core.commons.zahir_types import RootResult
from zahir.core.evaluate import JobContext, evaluate, setup


def returning_root(ctx: JobContext):
    yield from ()
    return {"result": 42}


def returning_root_with_emit(ctx: JobContext):
    yield EEmit("before")
    return {"result": 42}


def returning_none_root(ctx: JobContext):
    yield from ()
    return None  # noqa: PLR1711


def test_root_return_value_is_yielded_by_evaluate():
    """Proves evaluate yields the root job's return value wrapped in RootResult."""

    scope = {"returning_root": returning_root}
    result = root_value(evaluate(setup(n_workers=1), "returning_root", (), scope))

    assert result == {"result": 42}


def test_root_return_value_comes_after_emitted_events():
    """Proves the root return value appears after any EEmit events in the stream."""

    stream = list(
        evaluate(
            setup(n_workers=1),
            "returning_root_with_emit",
            (),
            {"returning_root_with_emit": returning_root_with_emit},
        )
    )

    assert user_events(stream) == ["before"]
    assert stream.index("before") < stream.index(RootResult({"result": 42}))


def test_root_return_none_is_still_surfaced():
    """Proves a root job returning None still yields a RootResult, so None is observable."""

    result = root_value(
        evaluate(
            setup(n_workers=1),
            "returning_none_root",
            (),
            {"returning_none_root": returning_none_root},
        )
    )

    assert result is None
