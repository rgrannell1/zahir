"""UX test: verifies that a root job's return value is surfaced by evaluate."""

from tertius import EEmit

from tests.shared import user_events
from zahir.core.evaluate import JobContext, evaluate, setup


def returning_root(ctx: JobContext):
    yield from ()
    return {"result": 42}  # noqa: B901


def returning_root_with_emit(ctx: JobContext):
    yield EEmit("before")
    return {"result": 42}  # noqa: B901


def returning_none_root(ctx: JobContext):
    yield from ()
    return None  # noqa: B901, PLR1711


def test_root_return_value_is_yielded_by_evaluate():
    """Proves evaluate yields the root job's return value as the final event."""

    scope = {"returning_root": returning_root}
    events = user_events(evaluate(setup(n_workers=1), "returning_root", (), scope))

    assert events == [{"result": 42}]


def test_root_return_value_comes_after_emitted_events():
    """Proves the root return value appears after any EEmit events in the stream."""

    events = user_events(
        evaluate(
            setup(n_workers=1),
            "returning_root_with_emit",
            (),
            {"returning_root_with_emit": returning_root_with_emit},
        )
    )

    assert events == ["before", {"result": 42}]


def test_root_return_none_yields_nothing_extra():
    """Proves a root job returning None does not add anything to the event stream."""

    events = user_events(
        evaluate(
            setup(n_workers=1),
            "returning_none_root",
            (),
            {"returning_none_root": returning_none_root},
        )
    )

    assert events == []
