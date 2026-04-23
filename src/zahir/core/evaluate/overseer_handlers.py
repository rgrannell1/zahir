from collections import deque
from collections.abc import Generator
from typing import Any

from zahir.core.constants import OverseerMessage as OM, WorkItemTag
from zahir.core.zahir_types import JobSpec, OverseerState


def _get_job(
    state: OverseerState, worker_pid_bytes: bytes
) -> Generator[Any, Any, tuple[OverseerState, Any]]:
    """Return a buffered result for this worker if one exists, otherwise the next queued job, otherwise None."""

    results = state.pending_results.get(worker_pid_bytes)
    if results:
        sequence_number, body = results.popleft()
        return state, (WorkItemTag.RESULT, sequence_number, body)

    if state.queue:
        job = state.queue.popleft()
        return state, (
            WorkItemTag.JOB,
            job.fn_name,
            job.args,
            job.reply_to,
            job.timeout_ms,
            job.sequence_number,
        )

    return state, None
    yield


def _acquire(state: OverseerState, name: str, limit: int) -> Generator[Any, Any, tuple[OverseerState, bool]]:
    """Try to acquire a concurrency slot for a given name. Returns True if successful, False if the limit has been reached."""

    current_limit, count = state.concurrency.get(name, (limit, 0))

    if count < current_limit:
        state.concurrency[name] = (current_limit, count + 1)
        return state, True

    return state, False
    yield


def _signal(state: OverseerState, name: str) -> Generator[Any, Any, tuple[OverseerState, str | None]]:
    """Get the current state of a named semaphore. Returns None if the semaphore has not been set."""
    return state, state.semaphores.get(name)
    yield


def _is_done(state: OverseerState) -> Generator[Any, Any, tuple[OverseerState, bool]]:
    """Check if all jobs are done. This is used by workers to determine if they should continue processing."""

    return state, state.pending == 0 and not state.queue
    yield


def _get_error(state: OverseerState) -> Generator[Any, Any, tuple[OverseerState, Exception | None]]:
    """Get the root error, if any. This is used by workers to check if they should abort early."""

    return state, state.root_error
    yield


def _enqueue(
    state: OverseerState,
    fn_name: str,
    args: tuple,
    reply_to: bytes | None,
    timeout_ms: int | None,
    sequence_number: int | None = None,
) -> Generator[Any, Any, OverseerState]:
    """Enqueue a new job to be processed. This is used to implement dynamic fan-out patterns, where the number of jobs is not known upfront."""

    spec = JobSpec(
        fn_name=fn_name,
        args=args,
        reply_to=reply_to,
        timeout_ms=timeout_ms,
        sequence_number=sequence_number,
    )
    state.queue.append(spec)
    state.pending += 1

    return state
    yield


def _job_done(
    state: OverseerState, reply_to_bytes: bytes | None, sequence_number: Any, body: Any
) -> Generator[Any, Any, OverseerState]:
    """Decrement pending and buffer the result for the waiting parent worker, if any. Root job results are stored on state."""

    state.pending -= 1

    if reply_to_bytes is None:
        state.root_result = body
    else:
        if reply_to_bytes not in state.pending_results:
            state.pending_results[reply_to_bytes] = deque()
        state.pending_results[reply_to_bytes].append((sequence_number, body))

    return state
    yield


def _get_result(state: OverseerState) -> Generator[Any, Any, tuple[OverseerState, Any]]:
    """Return the root job's return value."""

    return state, state.root_result
    yield


def _job_failed(state: OverseerState, error: Exception) -> Generator[Any, Any, OverseerState]:
    """Decrement pending and record the root error for a failed root job."""

    state.pending -= 1

    if state.root_error is None:
        state.root_error = error

    return state
    yield


def _release(state: OverseerState, name: str) -> Generator[Any, Any, OverseerState]:
    """Release a concurrency slot for a given name."""

    if name in state.concurrency:
        current_limit, count = state.concurrency[name]
        state.concurrency[name] = (current_limit, max(0, count - 1))

    return state
    yield


def _set_semaphore(state: OverseerState, name: str, sem_state: str) -> Generator[Any, Any, OverseerState]:
    """Set the state of a semaphore. This can be used to implement more complex synchronization patterns."""

    state.semaphores[name] = sem_state

    return state
    yield


def _dispatch(handlers: dict, state: OverseerState, body: Any) -> Any:
    """Dispatch a call or cast to the appropriate handler based on the body."""

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
