"""UX test: verifies that a root job's return value is surfaced by evaluate."""

from tertius import EEmit

from zahir.core.evaluate import JobContext, evaluate


def returning_root(ctx: JobContext):
    return {"result": 42}
    yield


def returning_root_with_emit(ctx: JobContext):
    yield EEmit("before")
    return {"result": 42}
    yield


def returning_none_root(ctx: JobContext):
    return None
    yield


def test_root_return_value_is_yielded_by_evaluate():
    """Proves evaluate yields the root job's return value as the final event."""

    events = list(evaluate("returning_root", (), {"returning_root": returning_root}, n_workers=1))

    assert events == [{"result": 42}]


def test_root_return_value_comes_after_emitted_events():
    """Proves the root return value appears after any EEmit events in the stream."""

    events = list(evaluate("returning_root_with_emit", (), {"returning_root_with_emit": returning_root_with_emit}, n_workers=1))

    assert events == ["before", {"result": 42}]


def test_root_return_none_yields_nothing_extra():
    """Proves a root job returning None does not add anything to the event stream."""

    events = list(evaluate("returning_none_root", (), {"returning_none_root": returning_none_root}, n_workers=1))

    assert events == []
