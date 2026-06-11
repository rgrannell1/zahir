from tertius import EEmit

from tests.shared import user_events
from zahir.core.evaluate import evaluate, setup


def emitting_job(ctx):
    yield EEmit({"status": "done"})


def test_job_emit_is_received_by_caller():
    """Proves a job that yields EEmit surfaces the event to the evaluate caller."""

    events = user_events(
        evaluate(setup(n_workers=1), "emitting_job", (), {"emitting_job": emitting_job})
    )

    assert events == [{"status": "done"}]
