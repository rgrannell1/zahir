from datetime import UTC, datetime

from tertius import EEmit

from zahir import CurrentTime, evaluate, setup, time_dependency

PAST = datetime(2000, 1, 1, tzinfo=UTC)
FUTURE = datetime(2100, 1, 1, tzinfo=UTC)
INJECTED_FUTURE = datetime(2200, 1, 1, tzinfo=UTC)


def provide_future_time(_coeffect: CurrentTime) -> datetime:
    """Provide a time beyond the dependency window."""

    return INJECTED_FUTURE


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

    events = list(evaluate(setup(n_workers=1), "job", (), {"job": job_with_impossible_time_dep}))

    assert {"impossible": True} in events


def test_satisfied_time_dependency_returns_satisfied_to_job():
    """Proves a satisfied time dependency returns a satisfied result to the job."""

    events = list(evaluate(setup(n_workers=1), "job", (), {"job": job_with_satisfied_time_dep}))

    assert {"satisfied": True} in events


def test_evaluate_passes_providers_to_workers():
    """Proves evaluate passes a replacement provider into each worker."""

    providers = {CurrentTime.tag: provide_future_time}
    cases = [
        ("process", setup(n_workers=1)),
        ("thread", setup(n_workers=0, n_thread_workers=1)),
    ]

    for name, runtime in cases:
        events = evaluate(
            runtime,
            "job",
            (),
            {"job": job_with_satisfied_time_dep},
            providers=providers,
        )
        assert {"satisfied": False} in list(events), name
