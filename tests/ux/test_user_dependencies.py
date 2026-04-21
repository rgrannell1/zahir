from datetime import UTC, datetime

from tertius import EEmit

from zahir.core.dependencies.time import time_dependency
from zahir.core.evaluate import evaluate


PAST = datetime(2000, 1, 1, tzinfo=UTC)
FUTURE = datetime(2100, 1, 1, tzinfo=UTC)


def job_with_impossible_time_dep(ctx):
    result = yield from time_dependency(before=PAST)
    match result:
        case ("impossible", _):
            yield EEmit({"impossible": True})
        case ("satisfied", _):
            yield EEmit({"impossible": False})


def job_with_satisfied_time_dep(ctx):
    result = yield from time_dependency(before=FUTURE, after=PAST)
    match result:
        case ("satisfied", _):
            yield EEmit({"satisfied": True})
        case ("impossible", _):
            yield EEmit({"satisfied": False})


def test_impossible_time_dependency_returns_impossible_to_job():
    """Proves an impossible time dependency returns an impossible result to the job."""

    events = list(
        evaluate("job", (), {"job": job_with_impossible_time_dep}, n_workers=1)
    )

    assert {"impossible": True} in events


def test_satisfied_time_dependency_returns_satisfied_to_job():
    """Proves a satisfied time dependency returns a satisfied result to the job."""

    events = list(
        evaluate("job", (), {"job": job_with_satisfied_time_dep}, n_workers=1)
    )

    assert {"satisfied": True} in events
