from zahir.core.dependencies.dependency import ImpossibleError, dependency
from zahir.core.dependencies.group import group_dependency
from zahir.core.effects import EImpossible, ESatisfied
from tertius import ESleep


def _always_satisfied():
    return (True, {"source": "a"})
    yield  # make it a generator function


def _always_impossible():
    raise ImpossibleError("blocked")
    yield  # make it a generator function


def _sleeps_then_satisfied():
    yield ESleep(ms=1000)
    return (True, {"source": "b"})


def _satisfied():
    return dependency(_always_satisfied)


def _impossible():
    return dependency(_always_impossible)


def _sleep_then_satisfied():
    return dependency(_sleeps_then_satisfied)


def _drive(gen):
    """Drive a generator to completion, collecting all yielded effects."""

    effects = []
    value = None
    try:
        effect = next(gen)
        while True:
            effects.append(effect)
            value = None
            effect = gen.send(value)
    except StopIteration:
        pass

    return effects


def test_empty_group_yields_esatisfied():
    """Proves an empty dependency group immediately yields ESatisfied."""

    effects = _drive(group_dependency([]))
    assert len(effects) == 1
    assert isinstance(effects[0], ESatisfied)


def test_single_satisfied_dependency_yields_satisfied():
    """Proves a single satisfied dependency passes its ESatisfied effect through."""

    effects = _drive(group_dependency([_satisfied()]))
    assert len(effects) == 1
    assert isinstance(effects[0], ESatisfied)


def test_multiple_satisfied_dependencies_all_pass_through():
    """Proves all effects from all dependencies are yielded in order."""

    effects = _drive(group_dependency([_satisfied(), _satisfied()]))
    assert len(effects) == 2
    assert all(isinstance(e, ESatisfied) for e in effects)


def test_impossible_dependency_short_circuits():
    """Proves an EImpossible in any dependency stops the group immediately."""

    effects = _drive(group_dependency([_impossible(), _satisfied()]))
    assert len(effects) == 1
    assert isinstance(effects[0], EImpossible)


def test_impossible_after_satisfied_short_circuits():
    """Proves short-circuit fires even when earlier dependencies were satisfied."""

    effects = _drive(group_dependency([_satisfied(), _impossible(), _satisfied()]))
    assert isinstance(effects[-1], EImpossible)
    assert not any(
        isinstance(e, ESatisfied) for e in effects[effects.index(effects[-1]):]
    )


def test_sleep_effects_are_passed_through():
    """Proves intermediate ESleep effects are yielded before final ESatisfied."""

    effects = _drive(group_dependency([_sleep_then_satisfied()]))
    assert isinstance(effects[0], ESleep)
    assert isinstance(effects[1], ESatisfied)


def test_handler_value_is_sent_back_into_dependency():
    """Proves values sent into the group are forwarded to the dependency generator."""

    received = []

    def _capturing_condition():
        val = yield ESleep(ms=1)
        received.append(val)
        return True

    gen = group_dependency([dependency(_capturing_condition)])
    effect = next(gen)
    assert isinstance(effect, ESleep)
    gen.send(42)  # resumes condition with val=42, then ESatisfied comes out
    try:
        next(gen)
    except StopIteration:
        pass

    assert received == [42]
