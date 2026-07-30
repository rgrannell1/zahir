"""Integration test: exception-typed values returned by jobs are values, not failures."""

from collections.abc import Generator
from typing import Any

from tertius import EEmit, ESleep

from tests.shared import user_events
from zahir.core.evaluate import JobContext, evaluate, setup
from zahir.core.exceptions import JobError


def make_error(ctx: JobContext) -> Generator[Any, Any, JobError]:
    yield ESleep(ms=1)
    return JobError(ValueError("payload"))


def receive_error(ctx: JobContext) -> Generator[Any, Any, None]:
    value = yield ctx.scope.make_error()
    yield EEmit(("received", type(value).__name__))


_SCOPE = {"make_error": make_error, "receive_error": receive_error}


def test_job_returning_error_value_is_not_treated_as_failure():
    """Proves a job can return an exception instance as a value without failing its parent."""

    events = user_events(evaluate(setup(n_workers=1), "receive_error", (), _SCOPE))
    assert ("received", "JobError") in events
