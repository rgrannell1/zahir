from orbis import Effect, Event

from zahir.core.effects import (
    EAcquire,
    EAwait,
    EImpossible,
    ESatisfied,
    ESetSemaphore,
    EGetSemaphore,
)


def test_esatisfied_is_event():
    """Proves ESatisfied is an Event."""

    assert isinstance(ESatisfied(), Event)


def test_esatisfied_default_metadata_is_empty_dict():
    """Proves ESatisfied metadata defaults to an empty dict."""

    assert ESatisfied().metadata == {}


def test_esatisfied_stores_metadata():
    """Proves ESatisfied stores arbitrary metadata."""

    effect = ESatisfied(metadata={"key": "value"})
    assert effect.metadata == {"key": "value"}


def test_eimpossible_is_event():
    """Proves EImpossible is an Event."""

    assert isinstance(EImpossible(reason="blocked"), Event)


def test_eimpossible_stores_reason():
    """Proves EImpossible stores its reason string."""

    effect = EImpossible(reason="too late")
    assert effect.reason == "too late"


def test_eawait_is_effect():
    """Proves EAwait is an Effect."""

    assert isinstance(EAwait(fn_name="my_fn"), Effect)


def test_eawait_stores_fn_name_and_args():
    """Proves EAwait stores fn_name and args."""

    effect = EAwait(fn_name="process", args=(1, 2))
    assert effect.fn_name == "process"
    assert effect.args == (1, 2)


def test_eawait_default_args_is_empty_tuple():
    """Proves EAwait args defaults to an empty tuple."""

    assert EAwait(fn_name="fn").args == ()


def test_eawait_default_timeout_ms_is_none():
    """Proves EAwait timeout_ms defaults to None."""

    assert EAwait(fn_name="fn").timeout_ms is None


def test_eawait_stores_timeout_ms():
    """Proves EAwait stores an explicit timeout_ms."""

    assert EAwait(fn_name="fn", timeout_ms=5000).timeout_ms == 5000


def test_eget_semaphore_is_effect():
    """Proves EGetSemaphore is an Effect."""

    assert isinstance(EGetSemaphore(name="db"), Effect)


def test_eget_semaphore_stores_name():
    """Proves EGetSemaphore stores its semaphore name."""

    assert EGetSemaphore(name="my-lock").name == "my-lock"


def test_eacquire_is_effect():
    """Proves EAcquire is an Effect."""

    assert isinstance(EAcquire(name="workers", limit=4), Effect)


def test_eacquire_stores_name_and_limit():
    """Proves EAcquire stores its name and concurrency limit."""

    effect = EAcquire(name="workers", limit=4)
    assert effect.name == "workers"
    assert effect.limit == 4


def test_esetsemaphore_is_event():
    """Proves ESetSemaphore is an Event."""

    assert isinstance(ESetSemaphore(name="db", state="satisfied"), Event)


def test_esetsemaphore_stores_name_and_state():
    """Proves ESetSemaphore stores its name and state."""

    effect = ESetSemaphore(name="db", state="satisfied")
    assert effect.name == "db"
    assert effect.state == "satisfied"


def test_effect_tags_are_unique():
    """Proves all effect tags are distinct from one another."""

    tags = [cls.tag for cls in (EAwait, EGetSemaphore, EAcquire)]
    assert len(tags) == len(set(tags))


def test_event_tags_are_unique():
    """Proves all event tags are distinct from one another."""

    tags = [cls.tag for cls in (ESatisfied, EImpossible, ESetSemaphore)]
    assert len(tags) == len(set(tags))
