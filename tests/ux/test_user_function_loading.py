import pytest

from tertius import EEmit

from evaluate import evaluate


def module_level_job():
    yield EEmit("ok")


def test_module_level_job_runs_successfully():
    """Proves a module-level job function can be loaded by worker processes."""

    events = list(evaluate("job", (), {"job": module_level_job}, n_workers=1))
    assert events == ["ok"]


def test_locally_defined_job_raises_not_hangs():
    """Proves a locally-defined job function fails with a clear error, not a silent hang."""

    def local_job():
        yield EEmit("ok")

    with pytest.raises(Exception):
        list(evaluate("job", (), {"job": local_job}, n_workers=1))
