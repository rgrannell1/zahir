"""UX test: verifies setup() produces a runtime handle accepted by evaluate()."""

import pytest

from tests.shared import user_events
from zahir.core.evaluate import JobContext, evaluate, setup


def setup_returning_root(ctx: JobContext):
    assert ctx is not None
    yield from ()
    return {"result": 42}  # noqa: B901


def test_setup_runtime_runs_the_existing_local_path():
    """Proves evaluate(setup(...), ...) preserves the current local execution path."""

    runtime = setup(n_workers=1)
    scope = {"setup_returning_root": setup_returning_root}

    events = user_events(evaluate(runtime, "setup_returning_root", (), scope))

    assert events == [{"result": 42}]


def test_setup_remote_refs_fail_fast_until_wired():
    """Proves remote process refs are rejected explicitly until runtime resolution lands."""

    runtime = setup(overseer=("name", "overseer"), workers=(("name", "worker-1"),), n_workers=1)
    scope = {"setup_returning_root": setup_returning_root}

    with pytest.raises(NotImplementedError, match="not wired into evaluate"):
        list(evaluate(runtime, "setup_returning_root", (), scope))
