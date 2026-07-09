"""Tests for the retried combinator: retry-on-failure, backoff, and input validation."""

import pytest
from tertius import EEmit, ESleep

from zahir.core.effects import EAwait, JobSpec, await_all
from zahir.core.exceptions import JobError, JobTimeoutError
from zahir.core.retry import retried


def make_spec(fn_name: str = "fetch") -> EAwait:
    """Build a scalar EAwait as ScopeProxy would."""

    return EAwait(jobs=[JobSpec(fn_name=fn_name)], scalar=True)


def answer_await(gen, outcome):
    """Throw Exception outcomes into the generator; send any other value."""

    if isinstance(outcome, Exception):
        return gen.throw(outcome)
    return gen.send(outcome)


def drive(gen, outcomes: list):
    """Drive retried(), answering each EAwait with the next outcome.

    An Exception outcome is thrown into the generator; any other value is sent.
    Non-EAwait effects (EEmit, ESleep) are recorded and answered with None.
    Returns (yielded_effects, return_value); raised errors propagate to the test.
    """

    yielded = []
    pending = list(outcomes)
    try:
        effect = next(gen)
        while True:
            yielded.append(effect)
            if isinstance(effect, EAwait):
                effect = answer_await(gen, pending.pop(0))
            else:
                effect = gen.send(None)
    except StopIteration as stop:
        return yielded, stop.value


RETRY_CASES = [
    {
        "description": "first attempt succeeds: one EAwait, no retries",
        "outcomes": ["result"],
        "expected_result": "result",
        "expected_awaits": 1,
        "expected_sleeps": [],
    },
    {
        "description": "one failure then success: retry emits telemetry and backs off",
        "outcomes": [JobError(ValueError("boom")), "result"],
        "expected_result": "result",
        "expected_awaits": 2,
        "expected_sleeps": [100],
    },
    {
        "description": "two failures then success: backoff grows linearly",
        "outcomes": [JobError(ValueError("a")), JobTimeoutError("slow"), "result"],
        "expected_result": "result",
        "expected_awaits": 3,
        "expected_sleeps": [100, 200],
    },
]


@pytest.mark.parametrize("case", RETRY_CASES, ids=lambda case: case["description"])
def test_retried_retries_until_success(case):
    """Proves retried re-dispatches the same EAwait after failures and returns the result."""

    spec = make_spec()
    gen = retried(spec, attempts=3, backoff_ms=100)
    yielded, result = drive(gen, case["outcomes"])

    awaits = [effect for effect in yielded if isinstance(effect, EAwait)]
    sleeps = [effect.ms for effect in yielded if isinstance(effect, ESleep)]
    emits = [effect for effect in yielded if isinstance(effect, EEmit)]

    assert result == case["expected_result"]
    assert len(awaits) == case["expected_awaits"]
    assert all(effect == spec for effect in awaits)
    assert sleeps == case["expected_sleeps"]
    assert len(emits) == len(case["expected_sleeps"])


def test_retried_raises_after_exhausting_attempts():
    """Proves the final failure propagates unchanged once attempts are exhausted."""

    final_error = JobError(ValueError("still broken"))
    gen = retried(make_spec(), attempts=2, backoff_ms=50)

    with pytest.raises(JobError) as err_info:
        drive(gen, [JobError(ValueError("first")), final_error])
    assert err_info.value is final_error


def test_retried_does_not_catch_non_job_errors():
    """Proves unrelated exceptions thrown into the await are not retried."""

    gen = retried(make_spec(), attempts=3, backoff_ms=50)

    with pytest.raises(KeyError):
        drive(gen, [KeyError("not a job failure")])


def test_retried_rejects_non_scalar_eawait():
    """Proves retried refuses multi-job EAwaits from await_all."""

    batch = await_all([make_spec("a"), make_spec("b")])
    gen = retried(batch, attempts=3, backoff_ms=50)

    with pytest.raises(ValueError, match="scalar"):
        next(gen)


def test_retried_rejects_zero_attempts():
    """Proves retried requires at least one attempt."""

    gen = retried(make_spec(), attempts=0, backoff_ms=50)

    with pytest.raises(ValueError, match="attempts"):
        next(gen)


def test_retried_emits_retry_event_with_attempt_and_error():
    """Proves the retry telemetry event carries the fn name, attempt number, and error type."""

    gen = retried(make_spec("encode"), attempts=2, backoff_ms=50)
    yielded, _ = drive(gen, [JobTimeoutError("slow"), "done"])

    emits = [effect for effect in yielded if isinstance(effect, EEmit)]
    assert len(emits) == 1
    dims = emits[0].body.dims
    assert dims["fn"] == ["encode"]
    assert dims["attempt"] == ["1"]
    assert dims["error"] == ["JobTimeoutError"]
