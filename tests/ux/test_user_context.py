from tertius import EEmit

from evaluate import evaluate, JobContext


class CustomContext(JobContext):
    def __init__(self):
        self.tag = "ctx-value"


def job_reading_context(ctx: CustomContext):
    yield EEmit({"tag": ctx.tag, "has_scope": hasattr(ctx, "_scope")})


def job_with_default_context(ctx: JobContext):
    yield EEmit({"has_scope": hasattr(ctx, "_scope")})



def test_custom_context_is_passed_as_first_arg():
    """Proves a custom context instance is created per worker and passed as the first job argument."""

    events = list(evaluate(
        "job_reading_context",
        (),
        {"job_reading_context": job_reading_context},
        n_workers=1,
        context=CustomContext,
    ))

    assert events == [{"tag": "ctx-value", "has_scope": True}]


def test_default_context_is_used_when_none_provided():
    """Proves the default JobContext is used when no context class is given."""

    events = list(evaluate(
        "job_with_default_context",
        (),
        {"job_with_default_context": job_with_default_context},
        n_workers=1,
    ))

    assert events == [{"has_scope": True}]
