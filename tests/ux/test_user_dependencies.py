from datetime import UTC, datetime

from tertius import EEmit

from zahir import (
    CurrentTime,
    FileExists,
    ResourceUsage,
    evaluate,
    file_dependency,
    resource_dependency,
    setup,
    time_dependency,
)

PAST = datetime(2000, 1, 1, tzinfo=UTC)
FUTURE = datetime(2100, 1, 1, tzinfo=UTC)
INJECTED_FUTURE = datetime(2200, 1, 1, tzinfo=UTC)


def provide_future_time(_coeffect: CurrentTime) -> datetime:
    """Provide a time beyond the dependency window."""

    return INJECTED_FUTURE


def provide_available_resource(_coeffect: ResourceUsage) -> float:
    """Provide a resource usage value that meets a zero-percent limit."""

    return 0.0


def provide_existing_file(_coeffect: FileExists) -> bool:
    """Provide a positive file-existence result."""

    return True


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


def job_with_resource_dep(ctx):
    result = yield from resource_dependency("memory", max_percent=0.0)
    yield EEmit({"resource_state": result[0]})


def job_with_file_dep(ctx):
    result = yield from file_dependency("/not/present/on/the/worker")
    yield EEmit({"file_state": result[0]})


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


def test_evaluate_passes_resource_provider_to_workers():
    """Proves process and thread workers accept a resource-usage provider."""

    providers = {ResourceUsage.tag: provide_available_resource}
    runtimes = [setup(n_workers=1), setup(n_workers=0, n_thread_workers=1)]

    for runtime in runtimes:
        events = evaluate(runtime, "job", (), {"job": job_with_resource_dep}, providers=providers)
        assert {"resource_state": "satisfied"} in list(events)


def test_evaluate_passes_file_provider_to_workers():
    """Proves process and thread workers accept a file-existence provider."""

    providers = {FileExists.tag: provide_existing_file}
    runtimes = [setup(n_workers=1), setup(n_workers=0, n_thread_workers=1)]

    for runtime in runtimes:
        events = evaluate(runtime, "job", (), {"job": job_with_file_dep}, providers=providers)
        assert {"file_state": "satisfied"} in list(events)
