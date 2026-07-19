"""Tests for SuspensionTable result collection: scalar, await_all, and gather_all modes."""

import pytest

from tests.evaluate.mocks import make_deadlined_parent
from zahir.core.effects import EAwait, JobSpec, await_all, gather_all
from zahir.core.evaluate.suspension import RunningJob, SuspensionTable
from zahir.core.exceptions import JobError, JobTimeoutError
from zahir.core.fp_types import Err, Ok
from zahir.core.zahir_types import ResultItem


def make_parent() -> RunningJob:
    """Build a minimal parent job to suspend."""

    return RunningJob(
        fn_name="parent", eval_gen=None, reply_to=None, parent_sequence_number=None
    )


def make_scalar(fn_name: str) -> EAwait:
    """Build a scalar EAwait as ScopeProxy would."""

    return EAwait(jobs=[JobSpec(fn_name=fn_name)], scalar=True)


def run_fanout(effect: EAwait, completions: list[tuple[int, object]]):
    """Suspend a parent on effect, then deliver child results.

    completions is a list of (child_input_index, body) in completion order.
    Returns the final SuspensionTable.resume result.
    """

    table = SuspensionTable()
    enqueues = list(table.suspend(effect, make_parent(), b"worker"))
    sequence_numbers = [enqueue.job.sequence_number for enqueue in enqueues]

    outcome = None
    for child_idx, body in completions:
        outcome = table.resume(ResultItem(sequence_numbers[child_idx], body))
    return outcome


BOOM = JobError(ValueError("boom"))
SLOW = JobTimeoutError("slow")

FANOUT_CASES = [
    {
        "description": "gather: all children succeed, results in input order",
        "effect": gather_all([make_scalar("a"), make_scalar("b")]),
        "completions": [(0, "left"), (1, "right")],
        "expected": Ok([Ok("left"), Ok("right")]),
    },
    {
        "description": "gather: a failed child arrives as Err without raising",
        "effect": gather_all([make_scalar("a"), make_scalar("b")]),
        "completions": [(0, BOOM), (1, "right")],
        "expected": Ok([Err(BOOM), Ok("right")]),
    },
    {
        "description": "gather: input order is preserved when children finish out of order",
        "effect": gather_all([make_scalar("a"), make_scalar("b")]),
        "completions": [(1, "right"), (0, "left")],
        "expected": Ok([Ok("left"), Ok("right")]),
    },
    {
        "description": "gather: timeouts arrive as Err like any other job failure",
        "effect": gather_all([make_scalar("a"), make_scalar("b")]),
        "completions": [(0, "left"), (1, SLOW)],
        "expected": Ok([Ok("left"), Err(SLOW)]),
    },
    {
        "description": "await_all: first error in input order wins, successes are discarded",
        "effect": await_all([make_scalar("a"), make_scalar("b")]),
        "completions": [(0, BOOM), (1, "right")],
        "expected": Err(BOOM),
    },
    {
        "description": "await_all: all successes collect into a plain list",
        "effect": await_all([make_scalar("a"), make_scalar("b")]),
        "completions": [(0, "left"), (1, "right")],
        "expected": Ok(["left", "right"]),
    },
    {
        "description": "scalar: a single child result is unwrapped directly",
        "effect": make_scalar("a"),
        "completions": [(0, "only")],
        "expected": Ok("only"),
    },
    {
        "description": "scalar: a single child failure is unwrapped to Err",
        "effect": make_scalar("a"),
        "completions": [(0, BOOM)],
        "expected": Err(BOOM),
    },
]


@pytest.mark.parametrize("case", FANOUT_CASES, ids=lambda case: case["description"])
def test_fanout_collection(case):
    """Proves each EAwait dispatch mode collects child results with its documented semantics."""

    outcome = run_fanout(case["effect"], case["completions"])

    assert outcome is not None
    resumed_job, result = outcome
    assert resumed_job.fn_name == "parent"
    assert result == case["expected"]


def test_resume_returns_none_while_children_outstanding():
    """Proves resume withholds the parent until every child has reported."""

    effect = gather_all([make_scalar("a"), make_scalar("b")])
    table = SuspensionTable()
    enqueues = list(table.suspend(effect, make_parent(), b"worker"))
    first_sequence_number = enqueues[0].job.sequence_number

    assert table.resume(ResultItem(first_sequence_number, "left")) is None


# expiry


def test_pop_expired_returns_timed_out_parent():
    """Proves a suspended parent whose deadline has passed is removed and returned."""

    table = SuspensionTable()
    list(table.suspend(make_scalar("a"), make_deadlined_parent(deadline=1.0), b"worker"))

    expired = table.pop_expired(now=2.0)
    assert expired is not None
    assert expired.fn_name == "parent"
    assert table.pop_expired(now=2.0) is None


def test_pop_expired_ignores_live_parents():
    """Proves parents without a deadline, or with an unexpired one, are left suspended."""

    table = SuspensionTable()
    list(table.suspend(make_scalar("a"), make_parent(), b"worker"))
    list(table.suspend(make_scalar("b"), make_deadlined_parent(deadline=10.0), b"worker"))

    assert table.pop_expired(now=2.0) is None


def test_late_result_after_expiry_is_discarded():
    """Proves a child result arriving after its parent expired is discarded, not a crash."""

    table = SuspensionTable()
    enqueues = list(table.suspend(make_scalar("a"), make_deadlined_parent(deadline=1.0), b"worker"))
    table.pop_expired(now=2.0)

    late = ResultItem(enqueues[0].job.sequence_number, "late")
    assert table.resume(late) is None


def test_resume_discards_unknown_sequence_numbers():
    """Proves resume ignores results for children it never registered."""

    table = SuspensionTable()
    assert table.resume(ResultItem(999, "stray")) is None


# partial fan-out failure


def test_failed_enqueue_cleans_up_child_registrations():
    """Proves a mid-fan-out enqueue failure unregisters the fan-out, so late results
    from already-enqueued children are discarded rather than crashing the worker."""

    table = SuspensionTable()
    effect = await_all([make_scalar("a"), make_scalar("b")])
    gen = table.suspend(effect, make_parent(), b"worker")

    first_enqueue = next(gen)
    with pytest.raises(RuntimeError):
        gen.throw(RuntimeError("enqueue failed"))

    late = ResultItem(first_enqueue.job.sequence_number, "late")
    assert table.resume(late) is None
    assert table.child_to_parent == {}
    assert table.waiting == {}
