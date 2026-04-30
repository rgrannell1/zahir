import pytest
from tertius import EEmit, EReceive

from tests.shared import user_events
from zahir.core.evaluate import JobContext, evaluate
from zahir.core.exceptions import InvalidEffectError


def job_yielding_non_effect(ctx: JobContext):
    yield 42


def job_yielding_receive(ctx: JobContext):
    yield EReceive()


def job_catching_invalid_effect(ctx: JobContext):
    try:
        yield 42
    except InvalidEffectError as err:
        yield EEmit({"caught": str(err)})


def test_yielding_non_effect_raises_invalid_effect():
    """Proves a job that yields a non-Effect value raises InvalidEffectError rather than hanging."""

    with pytest.raises(InvalidEffectError):
        list(
            evaluate(
                "job_yielding_non_effect",
                (),
                {"job_yielding_non_effect": job_yielding_non_effect},
                n_workers=1,
            )
        )


def test_yielding_ereceive_raises_invalid_effect():
    """Proves a job that yields EReceive directly raises InvalidEffectError rather than blocking forever."""

    with pytest.raises(InvalidEffectError):
        list(
            evaluate(
                "job_yielding_receive",
                (),
                {"job_yielding_receive": job_yielding_receive},
                n_workers=1,
            )
        )


def test_invalid_effect_is_catchable_in_job():
    """Proves a job can catch InvalidEffectError and continue executing."""

    events = user_events(
        evaluate(
            "job_catching_invalid_effect",
            (),
            {"job_catching_invalid_effect": job_catching_invalid_effect},
            n_workers=1,
        )
    )

    assert len(events) == 1
    assert "caught" in events[0]


def test_missing_fn_name_raises_before_spawn():
    """Proves evaluate raises KeyError immediately when fn_name is absent from scope."""

    with pytest.raises(KeyError, match="not found in scope"):
        list(evaluate("nonexistent", (), {}, n_workers=1))
