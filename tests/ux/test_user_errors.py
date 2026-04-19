from tertius import EEmit

from effects import EAwait
from evaluate import evaluate
from exceptions import JobError


def crashing_job(ctx):
    raise ValueError("something went wrong")
    yield


def job_awaiting_crash(ctx):
    try:
        yield EAwait(fn_name="crashing_job")
    except JobError as err:
        yield EEmit({"error": str(err.cause)})


def test_crashing_job_does_not_hang():
    """Proves a job that raises an unhandled exception does not hang the workflow."""

    list(evaluate("crashing_job", (), {"crashing_job": crashing_job}, n_workers=1))


def test_crashing_job_sends_job_error_to_awaiter():
    """Proves a job that crashes sends JobError to any process awaiting its result."""

    scope = {
        "crashing_job": crashing_job,
        "job_awaiting_crash": job_awaiting_crash,
    }
    events = list(evaluate("job_awaiting_crash", (), scope, n_workers=2))

    assert events == [{"error": "something went wrong"}]
