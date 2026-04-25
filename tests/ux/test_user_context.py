from dataclasses import dataclass

from tertius import EEmit

from zahir.core.evaluate import evaluate, JobContext
from tests.shared import user_events


@dataclass
class CustomData:
    tag: str = "ctx-value"


def job_reading_context(ctx: JobContext[CustomData]):
    yield EEmit({"tag": ctx.user_context.tag, "has_scope": hasattr(ctx, "_scope")})


def job_with_default_context(ctx: JobContext):
    yield EEmit({"has_scope": hasattr(ctx, "_scope"), "user_context_is_none": ctx.user_context is None})


def test_custom_user_context_is_passed_to_jobs():
    """Proves a custom user_context instance is created per worker and passed as the first job argument."""

    events = user_events(
        evaluate(
            "job_reading_context",
            (),
            {"job_reading_context": job_reading_context},
            n_workers=1,
            user_context=CustomData,
        )
    )

    assert events == [{"tag": "ctx-value", "has_scope": True}]


def test_default_context_has_no_user_context():
    """Proves the default JobContext has user_context=None when no factory is given."""

    events = user_events(
        evaluate(
            "job_with_default_context",
            (),
            {"job_with_default_context": job_with_default_context},
            n_workers=1,
        )
    )

    assert events == [{"has_scope": True, "user_context_is_none": True}]
