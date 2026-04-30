from orbis import complete, handle

from tests.shared import drain_to
from zahir.core.backends.memory import make_memory_storage_handlers
from zahir.core.effects import (
    EStorageAcquire,
    EStorageEnqueue,
    EStorageGetJob,
    EStorageIsDone,
    EStorageRelease,
)
from zahir.core.evaluate.overseer import _handle_call, _handle_cast, _init

# _init


def test_init_returns_none_as_state_immediately():
    """Proves _init is a no-op that returns None without yielding any effects."""

    _, return_value = drain_to(_init())
    assert return_value is None


# _handle_call


def test_handle_call_yields_body_effect():
    """Proves _handle_call yields the body unchanged for handle() to intercept."""

    storage_effect = EStorageGetJob(b"worker")
    gen = _handle_call(None, storage_effect)
    yielded = next(gen)
    assert yielded is storage_effect


def test_handle_call_returns_state_and_handler_result():
    """Proves _handle_call returns (state, result) using the value sent back by handle()."""

    gen = _handle_call("sentinel_state", EStorageAcquire("workers", 4))
    _, return_value = drain_to(gen, responses={EStorageAcquire: True})
    assert return_value == ("sentinel_state", True)


# _handle_cast


def test_handle_cast_yields_body_effect():
    """Proves _handle_cast yields the body unchanged for handle() to intercept."""

    storage_effect = EStorageRelease("workers")
    gen = _handle_cast(None, storage_effect)
    yielded = next(gen)
    assert yielded is storage_effect


def test_handle_cast_returns_state_unchanged():
    """Proves _handle_cast returns the original state after the storage effect is handled."""

    gen = _handle_cast("sentinel_state", EStorageRelease("workers"))
    _, return_value = drain_to(gen)
    assert return_value == "sentinel_state"


# round-trip via handle() + memory storage handlers


def _make_handlers():
    """Shared storage handler set for round-trip tests."""
    return make_memory_storage_handlers()


def test_init_cast_seeds_backend_via_storage_handlers():
    """Proves EStorageEnqueue sent as a cast seeds the backend correctly."""

    handlers = _make_handlers()
    complete(handle(_handle_cast(None, EStorageEnqueue("start", (1, 2), None, None, None)), **handlers))
    _, result = complete(handle(_handle_call(None, EStorageIsDone()), **handlers))
    assert result is False


def test_round_trip_enqueue_then_get_job():
    """Proves a job enqueued via a cast storage effect is retrievable via a call storage effect."""

    handlers = _make_handlers()

    # enqueue root job, enqueue a child job, then fetch them
    complete(handle(_handle_cast(None, EStorageEnqueue("root", (), None, None, None)), **handlers))
    complete(handle(_handle_cast(None, EStorageEnqueue("child", (42,), None, None, None)), **handlers))
    # root job is first in queue; consume it
    complete(handle(_handle_call(None, EStorageGetJob(b"worker")), **handlers))
    # now child job should be available
    _, work = complete(handle(_handle_call(None, EStorageGetJob(b"worker")), **handlers))
    assert work[1] == "child"
