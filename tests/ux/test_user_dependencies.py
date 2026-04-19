from datetime import UTC, datetime

from tertius import EEmit

from zahir.core.dependencies.time import time_dependency
from zahir.core.effects import EImpossible, ESatisfied
from zahir.core.evaluate import evaluate


PAST = datetime(2000, 1, 1, tzinfo=UTC)
FUTURE = datetime(2100, 1, 1, tzinfo=UTC)


def job_with_impossible_time_dep(ctx):
    result = yield from time_dependency(before=PAST)
    yield EEmit({"impossible": isinstance(result, EImpossible)})


def job_with_satisfied_time_dep(ctx):
    result = yield from time_dependency(before=FUTURE, after=PAST)
    yield EEmit({"satisfied": isinstance(result, ESatisfied)})


def test_impossible_time_dependency_returns_eimpossible_to_job():
    """Proves an impossible time dependency returns EImpossible to the job for introspection."""

    events = list(
        evaluate("job", (), {"job": job_with_impossible_time_dep}, n_workers=1)
    )

    assert events == [{"impossible": True}]


def test_satisfied_time_dependency_returns_esatisfied_to_job():
    """Proves a satisfied time dependency returns ESatisfied to the job for introspection."""

    events = list(
        evaluate("job", (), {"job": job_with_satisfied_time_dep}, n_workers=1)
    )

    assert events == [{"satisfied": True}]
