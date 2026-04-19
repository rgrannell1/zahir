from effects import EImpossible, ESatisfied
from dependencies.group import group_dependency
from tertius import ESleep


def _satisfied():
    yield ESatisfied(metadata={"source": "a"})


def _impossible():
    yield EImpossible(reason="blocked")


def _sleep_then_satisfied():
    yield ESleep(ms=1000)
    yield ESatisfied(metadata={"source": "b"})


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


def test_empty_group_yields_nothing():
    """Proves an empty dependency group yields no effects."""

    assert _drive(group_dependency([])) == []


def test_single_satisfied_dependency_yields_satisfied():
    """Proves a single satisfied dependency passes its effect through."""

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
        isinstance(e, ESatisfied) for e in effects[effects.index(effects[-1]) :]
    )


def test_sleep_effects_are_passed_through():
    """Proves intermediate ESleep effects are yielded before final ESatisfied."""

    effects = _drive(group_dependency([_sleep_then_satisfied()]))
    assert isinstance(effects[0], ESleep)
    assert isinstance(effects[1], ESatisfied)


def test_handler_value_is_sent_back_into_dependency():
    """Proves values sent into the group are forwarded to the dependency generator."""

    received = []

    def _capturing():
        val = yield ESatisfied()
        received.append(val)

    gen = group_dependency([_capturing()])
    effect = next(gen)
    try:
        gen.send(42)
    except StopIteration:
        pass

    assert received == [42]
