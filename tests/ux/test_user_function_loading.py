import pytest
from tertius import EEmit

from tests.shared import user_events
from zahir.core.evaluate import evaluate, setup


def module_level_job(ctx):
    yield EEmit("ok")


def test_module_level_job_runs_successfully():
    """Proves a module-level job function can be loaded by worker processes."""

    events = user_events(evaluate(setup(n_workers=1), "job", (), {"job": module_level_job}))
    assert events == ["ok"]


def test_locally_defined_job_raises_not_hangs():
    """Proves a locally-defined job function fails with a clear error, not a silent hang."""

    def local_job():
        yield EEmit("ok")

    with pytest.raises(Exception):  # noqa: B017
        list(evaluate(setup(n_workers=1), "job", (), {"job": local_job}))
