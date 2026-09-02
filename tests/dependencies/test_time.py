from collections.abc import Generator
from datetime import datetime
from itertools import islice
from typing import Any

import time_machine
from orbis import Orbis
from tertius import EEmit, ESleep

from tests.shared import FUTURE, NOW, PAST, drain_to, root_value
from zahir import JobContext, evaluate, setup
from zahir.core.coeffects import CurrentTime, build_default_providers
from zahir.core.commons.zahir_types import DependencyResult
from zahir.core.dependencies.time import time_dependency


def test_time_dependency_requests_current_time():
    """Proves the time dependency obtains the current time through a coeffect."""

    assert isinstance(next(time_dependency()), CurrentTime)


def provide_fixed_time(_coeffect: CurrentTime) -> datetime:
    """Provide a fixed time for dependency tests."""

    return NOW


def test_time_dependency_accepts_replacement_provider():
    """Proves a caller can replace the clock without patching global time."""

    runtime = Orbis(providers={CurrentTime.tag: provide_fixed_time})
    emits, _return_value = drain_to(runtime(time_dependency(before=PAST)), EEmit)
    assert emits[0].body[0] == "impossible"


def interpret_time_dependency(
    before: datetime | None = None,
    after: datetime | None = None,
) -> Generator[Any, Any, DependencyResult]:
    """Apply the worker's default contextual providers to a time dependency."""

    runtime = Orbis(providers=build_default_providers())
    return runtime(time_dependency(before=before, after=after))


def run_time_job(ctx: JobContext):
    """Run one time dependency inside a worker."""

    yield from time_dependency()
    return "done"


def test_worker_provides_current_time():
    """Proves each worker supplies the current-time coeffect."""

    scope = {"run_time_job": run_time_job}
    events = evaluate(setup(n_workers=1), "run_time_job", (), scope)
    assert root_value(events) == "done"


@time_machine.travel(NOW, tick=False)
def test_no_constraints_emits_satisfied():
    """Proves time_dependency with no constraints is immediately satisfied."""

    emit = next(interpret_time_dependency(before=None, after=None))
    assert isinstance(emit, EEmit)
    assert emit.body[0] == "satisfied"


@time_machine.travel(NOW, tick=False)
def test_before_not_yet_passed_emits_satisfied():
    """Proves a future before constraint does not block satisfaction."""

    emit = next(interpret_time_dependency(before=FUTURE, after=None))
    assert emit.body[0] == "satisfied"


@time_machine.travel(NOW, tick=False)
def test_before_passed_emits_impossible():
    """Proves a past before constraint emits impossible."""

    emit = next(interpret_time_dependency(before=PAST, after=None))
    assert emit.body[0] == "impossible"


@time_machine.travel(NOW, tick=False)
def test_after_not_yet_reached_yields_sleep():
    """Proves a future after constraint yields ESleep via the polling loop."""

    effects = list(islice(interpret_time_dependency(before=None, after=FUTURE), 5))
    assert any(isinstance(e, ESleep) for e in effects)


def test_after_not_yet_reached_satisfies_once_time_arrives():
    """Proves the dependency satisfies once the after time has passed."""

    with time_machine.travel(NOW, tick=False):
        gen = interpret_time_dependency(before=None, after=FUTURE)
        next(gen)  # advance through one retry: EEmit(waiting)
        next(gen)  # advance through one retry: ESleep

    with time_machine.travel(FUTURE, tick=False):
        emits, _ = drain_to(gen, EEmit)

    assert emits[0].body[0] == "satisfied"


@time_machine.travel(NOW, tick=False)
def test_after_already_passed_emits_satisfied():
    """Proves a past after constraint is immediately satisfied."""

    emit = next(interpret_time_dependency(before=None, after=PAST))
    assert emit.body[0] == "satisfied"


@time_machine.travel(NOW, tick=False)
def test_before_and_after_both_satisfied():
    """Proves both constraints satisfied when now is within the window."""

    emit = next(interpret_time_dependency(before=FUTURE, after=PAST))
    assert emit.body[0] == "satisfied"


@time_machine.travel(NOW, tick=False)
def test_impossible_includes_timestamps():
    """Proves the impossible reason includes the violated before timestamp."""

    emit = next(interpret_time_dependency(before=PAST, after=None))
    assert emit.body[0] == "impossible"
    assert PAST.isoformat() in emit.body[1]["reason"]


# return values


@time_machine.travel(NOW, tick=False)
def test_impossible_returns_tuple_as_generator_value():
    """Proves the generator returns the impossible tuple as its StopIteration value."""

    emits, return_value = drain_to(interpret_time_dependency(before=PAST), EEmit)
    assert return_value is emits[0].body


@time_machine.travel(NOW, tick=False)
def test_satisfied_returns_tuple_as_generator_value():
    """Proves the generator returns the satisfied tuple as its StopIteration value."""

    emits, return_value = drain_to(interpret_time_dependency(before=FUTURE), EEmit)
    assert return_value is emits[0].body
