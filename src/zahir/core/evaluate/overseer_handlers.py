# Overseer gen_server handlers — each delegates its operation to the backend object that is the gen_server state.
from collections.abc import Generator
from typing import Any

from zahir.core.constants import OverseerMessage as OM


def _get_job(state, worker_pid_bytes: bytes) -> Generator[Any, Any, tuple]:
    """Return the next work item for this worker from the backend."""
    return state, state.get_job(worker_pid_bytes)
    yield


def _acquire(state, name: str, limit: int) -> Generator[Any, Any, tuple]:
    """Try to acquire a concurrency slot; return True if granted."""
    return state, state.acquire(name, limit)
    yield


def _signal(state, name: str) -> Generator[Any, Any, tuple]:
    """Return the current semaphore state for the given name."""
    return state, state.signal(name)
    yield


def _is_done(state) -> Generator[Any, Any, tuple]:
    """Return True if all jobs have completed."""
    return state, state.is_done()
    yield


def _get_error(state) -> Generator[Any, Any, tuple]:
    """Return the root error, if any."""
    return state, state.get_error()
    yield


def _get_result(state) -> Generator[Any, Any, tuple]:
    """Return the root job's return value."""
    return state, state.get_result()
    yield


def _enqueue(state, fn_name: str, args: tuple, reply_to: bytes | None, timeout_ms: int | None, sequence_number: int | None = None) -> Generator[Any, Any, Any]:
    """Enqueue a new child job."""
    state.enqueue(fn_name, args, reply_to, timeout_ms, sequence_number)
    return state
    yield


def _job_done(state, reply_to_bytes: bytes | None, sequence_number: Any, body: Any) -> Generator[Any, Any, Any]:
    """Record job completion and buffer the result for the waiting parent."""
    state.job_done(reply_to_bytes, sequence_number, body)
    return state
    yield


def _job_failed(state, error: Exception) -> Generator[Any, Any, Any]:
    """Record job failure."""
    state.job_failed(error)
    return state
    yield


def _release(state, name: str) -> Generator[Any, Any, Any]:
    """Release a concurrency slot."""
    state.release(name)
    return state
    yield


def _set_semaphore(state, name: str, sem_state: str) -> Generator[Any, Any, Any]:
    """Set the semaphore state."""
    state.set_semaphore(name, sem_state)
    return state
    yield


def _dispatch(handlers: dict, state, body: Any) -> Any:
    """Dispatch a call or cast to the appropriate handler based on the message tag."""
    key = body if isinstance(body, str) else body[0]
    args = body[1:] if isinstance(body, tuple) else ()
    return handlers[key](state, *args)


# request-response handlers (mcall)
CALL_HANDLERS = {
    OM.GET_JOB: _get_job,
    OM.ACQUIRE: _acquire,
    OM.SIGNAL: _signal,
    OM.IS_DONE: _is_done,
    OM.GET_ERROR: _get_error,
    OM.GET_RESULT: _get_result,
}

# fire-and-forget handlers (mcast)
CAST_HANDLERS = {
    OM.ENQUEUE: _enqueue,
    OM.JOB_DONE: _job_done,
    OM.JOB_FAILED: _job_failed,
    OM.RELEASE: _release,
    OM.SET_SEMAPHORE: _set_semaphore,
}
