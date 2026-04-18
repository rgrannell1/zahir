from tertius import EEmit

from evaluate import evaluate


def emitting_job():
    yield EEmit({"status": "done"})


def test_job_emit_is_received_by_caller():
    """Proves a job that yields EEmit surfaces the event to the evaluate caller."""

    events = list(evaluate("emitting_job", (), {"emitting_job": emitting_job}, n_workers=1))

    assert events == [{"status": "done"}]
