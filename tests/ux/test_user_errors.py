import pytest
from tertius import EEmit

from tests.shared import user_events
from zahir.core.evaluate import JobContext, evaluate, setup
from zahir.core.exceptions import JobError


def crashing_job(ctx: JobContext):
    yield from ()
    raise ValueError("something went wrong")


def job_awaiting_crash(ctx: JobContext):
    try:
        yield ctx.scope.crashing_job()
    except JobError as err:
        yield EEmit({"error": str(err.cause)})


def test_crashing_root_job_raises_job_error():
    """Proves a root job that raises propagates JobError to the evaluate caller."""

    with pytest.raises(JobError) as exc_info:
        list(evaluate(setup(n_workers=1), "crashing_job", (), {"crashing_job": crashing_job}))

    assert str(exc_info.value.cause) == "something went wrong"


def test_crashing_job_sends_job_error_to_awaiter():
    """Proves a job that crashes sends JobError to any process awaiting its result."""

    scope = {
        "crashing_job": crashing_job,
        "job_awaiting_crash": job_awaiting_crash,
    }
    events = user_events(evaluate(setup(n_workers=2), "job_awaiting_crash", (), scope))

    assert events == [{"error": "something went wrong"}]
