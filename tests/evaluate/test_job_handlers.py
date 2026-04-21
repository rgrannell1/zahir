import pytest

from zahir.core.effects import (
    EAcquire,
    EAcquireSlot,
    EGetSemaphore,
    ESetSemaphore,
    ESetSemaphoreState,
    ESignal,
)
from zahir.core.evaluate.job_handlers import (
    JobHandlerContext,
    _handle_acquire,
    _handle_set_semaphore,
    _handle_signal,
    make_job_handlers,
)
from zahir.core.exceptions import JobError, JobTimeout
from tests.evaluate.mocks import OVERSEER

CTX = JobHandlerContext()


# _handle_acquire


def test_handle_acquire_yields_eacquire_slot():
    """Proves _handle_acquire yields EAcquireSlot with the correct name and limit."""

    gen = _handle_acquire(JobHandlerContext(), EAcquire(name="workers", limit=4))
    effect = next(gen)
    assert effect == EAcquireSlot(name="workers", limit=4)


def test_handle_acquire_returns_true_and_tracks_name():
    """Proves _handle_acquire appends the name to acquired when slot is granted."""

    acquired = []
    gen = _handle_acquire(
        JobHandlerContext(acquired=acquired), EAcquire(name="workers", limit=4)
    )
    next(gen)  # EAcquireSlot
    with pytest.raises(StopIteration) as exc:
        gen.send(True)
    assert exc.value.value is True
    assert acquired == ["workers"]


def test_handle_acquire_returns_false_and_does_not_track():
    """Proves _handle_acquire does not append to acquired when slot is denied."""

    acquired = []
    gen = _handle_acquire(
        JobHandlerContext(acquired=acquired), EAcquire(name="workers", limit=4)
    )
    next(gen)  # EAcquireSlot
    with pytest.raises(StopIteration) as exc:
        gen.send(False)
    assert exc.value.value is False
    assert acquired == []


# _handle_signal


def test_handle_signal_yields_esignal():
    """Proves _handle_signal yields ESignal with the correct semaphore name."""

    gen = _handle_signal(CTX, EGetSemaphore(name="db"))
    assert next(gen) == ESignal(name="db")


def test_handle_signal_returns_semaphore_state():
    """Proves _handle_signal returns whatever state the overseer sends back."""

    gen = _handle_signal(CTX, EGetSemaphore(name="db"))
    next(gen)  # ESignal
    with pytest.raises(StopIteration) as exc:
        gen.send("satisfied")
    assert exc.value.value == "satisfied"


# _handle_set_semaphore


def test_handle_set_semaphore_yields_eset_semaphore_state():
    """Proves _handle_set_semaphore yields ESetSemaphoreState with the correct name and state."""

    gen = _handle_set_semaphore(CTX, ESetSemaphore(name="db", state="impossible"))
    assert next(gen) == ESetSemaphoreState(name="db", state="impossible")


# make_handlers


def test_make_handlers_contains_all_effect_types():
    """Proves make_handlers returns entries for all handled effect types. EAwait/EAwaitAll are handled by the worker body, not here."""

    handlers = make_job_handlers(JobHandlerContext())
    assert set(handlers.keys()) == {
        EAcquire.tag,
        EGetSemaphore.tag,
        ESetSemaphore.tag,
    }


def test_make_handlers_returns_callables():
    """Proves every handler value in make_handlers is callable."""

    handlers = make_job_handlers(JobHandlerContext())
    assert all(callable(h) for h in handlers.values())
