from collections import deque
from collections.abc import Generator
from typing import Any

from tertius import GenServer

from constants import ACQUIRE, ENQUEUE, GET_JOB, IS_DONE, JOB_DONE, RELEASE, SET_SEMAPHORE, SIGNAL
from zahir_types import JobSpec, OverseerState


def _get_job(state: OverseerState) -> tuple[OverseerState, Any]:
    if state.queue:
        job = state.queue.popleft()
        return state, (job.fn_name, job.args, job.reply_to, job.timeout_ms)
    return state, None


def _acquire(state: OverseerState, name: str, limit: int) -> tuple[OverseerState, bool]:

    current_limit, count = state.concurrency.get(name, (limit, 0))
    if count < current_limit:
        state.concurrency[name] = (current_limit, count + 1)
        return state, True

    return state, False


def _signal(state: OverseerState, name: str) -> tuple[OverseerState, str]:
    return state, state.semaphores.get(name, "satisfied")


def _is_done(state: OverseerState) -> tuple[OverseerState, bool]:
    return state, state.pending == 0 and not state.queue


def _enqueue(state: OverseerState, fn_name: str, args: tuple, reply_to: bytes | None, timeout_ms: int | None) -> OverseerState:
    state.queue.append(JobSpec(fn_name=fn_name, args=args, reply_to=reply_to, timeout_ms=timeout_ms))
    state.pending += 1
    return state


def _job_done(state: OverseerState) -> OverseerState:
    state.pending -= 1
    return state


def _release(state: OverseerState, name: str) -> OverseerState:
    if name in state.concurrency:
        current_limit, count = state.concurrency[name]
        state.concurrency[name] = (current_limit, max(0, count - 1))
    return state


def _set_semaphore(state: OverseerState, name: str, sem_state: str) -> OverseerState:
    state.semaphores[name] = sem_state
    return state


def _dispatch(handlers: dict, state: OverseerState, body: Any) -> Any:
    key = body if isinstance(body, str) else body[0]
    args = body[1:] if isinstance(body, tuple) else ()
    return handlers[key](state, *args)


_CALL_HANDLERS = {
    GET_JOB: _get_job,
    ACQUIRE:  _acquire,
    SIGNAL:   _signal,
    IS_DONE:  _is_done,
}

_CAST_HANDLERS = {
    ENQUEUE:      _enqueue,
    JOB_DONE:     _job_done,
    RELEASE:      _release,
    SET_SEMAPHORE: _set_semaphore,
}


class Overseer(GenServer[OverseerState]):
    def init(self, initial_fn: str, initial_args: tuple) -> OverseerState:
        job = JobSpec(fn_name=initial_fn, args=initial_args, reply_to=None)

        return OverseerState(
            queue=deque([job]),
            concurrency={},
            semaphores={},
            pending=1,
        )

    def handle_call(self, state: OverseerState, body: Any) -> tuple[OverseerState, Any]:
        return _dispatch(_CALL_HANDLERS, state, body)

    def handle_cast(self, state: OverseerState, body: Any) -> OverseerState:
        return _dispatch(_CAST_HANDLERS, state, body)


def run_overseer(initial_fn: str, initial_args: tuple) -> Generator[Any, Any, None]:
    yield from Overseer().loop(initial_fn, initial_args)
