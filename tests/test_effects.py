from orbis import Effect, Event

from zahir.core.effects import (
    EAcquire,
    EAwait,
    EAwaitAll,
    ESetSemaphore,
    EGetSemaphore,
    JobSpec,
)


def _spec(fn_name="fn", args=(), timeout_ms=None):
    return JobSpec(fn_name=fn_name, args=args, timeout_ms=timeout_ms)


def test_eawait_is_effect():
    """Proves EAwait is an Effect."""

    assert isinstance(EAwait(jobs=[_spec()]), Effect)


def test_eawait_stores_job_spec():
    """Proves EAwait stores the JobSpec list."""

    spec = _spec("process", (1, 2))
    effect = EAwait(jobs=[spec])
    assert effect.jobs == [spec]


def test_eawait_scalar_defaults_to_true():
    """Proves EAwait scalar defaults to True for single-job dispatch."""

    assert EAwait(jobs=[_spec()]).scalar is True


def test_eawait_all_returns_non_scalar_eawait():
    """Proves EAwaitAll produces an EAwait with scalar=False."""

    specs = [EAwait(jobs=[_spec("a")]), EAwait(jobs=[_spec("b")])]
    effect = EAwaitAll(specs)
    assert isinstance(effect, EAwait)
    assert effect.scalar is False


def test_eawait_all_preserves_job_order():
    """Proves EAwaitAll preserves the order of job specs."""

    specs = [EAwait(jobs=[_spec("a")]), EAwait(jobs=[_spec("b")])]
    effect = EAwaitAll(specs)
    assert [j.fn_name for j in effect.jobs] == ["a", "b"]


def test_jobspec_stores_fn_name_and_args():
    """Proves JobSpec stores fn_name and args."""

    spec = JobSpec(fn_name="process", args=(1, 2))
    assert spec.fn_name == "process"
    assert spec.args == (1, 2)


def test_jobspec_default_args_is_empty_tuple():
    """Proves JobSpec args defaults to an empty tuple."""

    assert JobSpec(fn_name="fn").args == ()


def test_jobspec_default_timeout_ms_is_none():
    """Proves JobSpec timeout_ms defaults to None."""

    assert JobSpec(fn_name="fn").timeout_ms is None


def test_jobspec_stores_timeout_ms():
    """Proves JobSpec stores an explicit timeout_ms."""

    assert JobSpec(fn_name="fn", timeout_ms=5000).timeout_ms == 5000


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

    tags = [cls.tag for cls in (ESetSemaphore,)]
    assert len(tags) == len(set(tags))
