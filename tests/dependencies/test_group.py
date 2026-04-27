from tertius import EEmit, ESleep

from zahir.core.dependencies.dependency import ImpossibleError, dependency
from zahir.core.dependencies.group import group_dependency
from tests.shared import drain_to


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


def test_empty_group_emits_satisfied():
    """Proves an empty dependency group immediately emits satisfied."""

    effects, _ = drain_to(group_dependency([]))
    assert len(effects) == 1
    assert isinstance(effects[0], EEmit)
    assert effects[0].body[0] == "satisfied"


def test_single_satisfied_dependency_emits_satisfied():
    """Proves a single satisfied dependency passes its satisfied emit through."""

    effects, _ = drain_to(group_dependency([_satisfied()]))
    user_emits = [e for e in effects if isinstance(e, EEmit) and type(e.body) is tuple]
    assert len(user_emits) == 1
    assert user_emits[0].body[0] == "satisfied"


def test_multiple_satisfied_dependencies_all_pass_through():
    """Proves all emits from all dependencies are yielded in order."""

    effects, _ = drain_to(group_dependency([_satisfied(), _satisfied()]))
    user_emits = [e for e in effects if isinstance(e, EEmit) and type(e.body) is tuple]
    assert len(user_emits) == 2
    assert all(e.body[0] == "satisfied" for e in user_emits)


def test_impossible_dependency_short_circuits():
    """Proves an impossible result in any dependency stops the group immediately."""

    effects, _ = drain_to(group_dependency([_impossible(), _satisfied()]))
    user_emits = [e for e in effects if isinstance(e, EEmit) and type(e.body) is tuple]
    assert len(user_emits) == 1
    assert user_emits[0].body[0] == "impossible"


def test_impossible_after_satisfied_short_circuits():
    """Proves short-circuit fires even when earlier dependencies were satisfied."""

    effects, _ = drain_to(group_dependency([_satisfied(), _impossible(), _satisfied()]))
    user_emits = [e for e in effects if isinstance(e, EEmit) and type(e.body) is tuple]
    assert user_emits[-1].body[0] == "impossible"
    assert not any(
        e.body[0] == "satisfied" for e in user_emits[user_emits.index(user_emits[-1]):]
    )


def test_sleep_effects_are_passed_through():
    """Proves intermediate ESleep effects are yielded before the final satisfied emit."""

    effects, _ = drain_to(group_dependency([_sleep_then_satisfied()]))
    assert any(isinstance(e, ESleep) for e in effects)
    user_emits = [e for e in effects if isinstance(e, EEmit) and type(e.body) is tuple]
    assert user_emits[0].body[0] == "satisfied"


def test_handler_value_is_sent_back_into_dependency():
    """Proves values sent into the group are forwarded to the dependency generator."""

    received = []

    def _capturing_condition():
        val = yield ESleep(ms=1)
        received.append(val)
        return True

    gen = group_dependency([dependency(_capturing_condition)])
    drain_to(gen, responses={ESleep: 42})

    assert received == [42]
