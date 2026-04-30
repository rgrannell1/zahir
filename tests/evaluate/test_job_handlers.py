from tests.shared import drain_to
from zahir.core.effects import (
    EAcquire,
    EAcquireSlot,
    EGetSemaphore,
    ESetSemaphore,
    ESetSemaphoreState,
    ESignal,
)
from zahir.core.evaluate.job_handlers import (
    _handle_acquire,
    _handle_set_semaphore,
    _handle_signal,
    make_job_handlers,
)
from zahir.core.evaluate.suspension import RunningJob, WorkerLocals


def _make_locals(acquired: list | None = None) -> WorkerLocals:
    """Build a WorkerLocals with a minimal RunningJob for handler tests."""
    acquired_slots = [] if acquired is None else acquired
    job = RunningJob(fn_name="test", eval_gen=None, reply_to=None, parent_sequence_number=None, acquired=acquired_slots)
    return WorkerLocals(current_job=job)


# _handle_acquire


def test_handle_acquire_yields_eacquire_slot():
    """Proves _handle_acquire yields EAcquireSlot with the correct name and limit."""

    gen = _handle_acquire(_make_locals(), EAcquire(name="workers", limit=4))
    effect = next(gen)
    assert effect == EAcquireSlot(name="workers", limit=4)


def test_handle_acquire_returns_true_and_tracks_name():
    """Proves _handle_acquire appends the name to acquired when slot is granted."""

    acquired = []
    gen = _handle_acquire(_make_locals(acquired), EAcquire(name="workers", limit=4))
    _, return_value = drain_to(gen, responses={EAcquireSlot: True})
    assert return_value is True
    assert acquired == ["workers"]


def test_handle_acquire_returns_false_and_does_not_track():
    """Proves _handle_acquire does not append to acquired when slot is denied."""

    acquired = []
    gen = _handle_acquire(_make_locals(acquired), EAcquire(name="workers", limit=4))
    _, return_value = drain_to(gen, responses={EAcquireSlot: False})
    assert return_value is False
    assert acquired == []


# _handle_signal


def test_handle_signal_yields_esignal():
    """Proves _handle_signal yields ESignal with the correct semaphore name."""

    gen = _handle_signal(EGetSemaphore(name="db"))
    assert next(gen) == ESignal(name="db")


def test_handle_signal_returns_semaphore_state():
    """Proves _handle_signal returns whatever state the overseer sends back."""

    gen = _handle_signal(EGetSemaphore(name="db"))
    _, return_value = drain_to(gen, responses={ESignal: "satisfied"})
    assert return_value == "satisfied"


# _handle_set_semaphore


def test_handle_set_semaphore_yields_eset_semaphore_state():
    """Proves _handle_set_semaphore yields ESetSemaphoreState with the correct name and state."""

    gen = _handle_set_semaphore(ESetSemaphore(name="db", state="impossible"))
    assert next(gen) == ESetSemaphoreState(name="db", state="impossible")


# make_handlers


def test_make_handlers_contains_all_effect_types():
    """Proves make_handlers returns entries for all handled effect types. EAwait/EAwaitAll are handled by the worker body, not here."""  # noqa: E501

    handlers = make_job_handlers(_make_locals(), [])
    assert set(handlers.keys()) == {
        EAcquire.tag,
        EGetSemaphore.tag,
        ESetSemaphore.tag,
    }


def test_make_handlers_returns_callables():
    """Proves every handler value in make_handlers is callable."""

    handlers = make_job_handlers(_make_locals(), [])
    assert all(callable(h) for h in handlers.values())
