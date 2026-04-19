from collections.abc import Generator
from functools import partial
from typing import Any

from tertius import EReceive, ESelf, Pid, mcall, mcast

from constants import ACQUIRE, ENQUEUE, RELEASE, SET_SEMAPHORE, SIGNAL
from effects import EAcquire, EAwait, EImpossible, ESatisfied, ESetSemaphore, ESignal
from exceptions import JobError, JobTimeout


_TIMEOUT_SENTINEL = "__timeout__"


def _handle_event(effect: ESatisfied | EImpossible) -> Generator[Any, Any, ESatisfied | EImpossible]:
    return effect
    yield


def _handle_await(overseer: Pid, effect: EAwait) -> Generator[Any, Any, Any]:
    me: Pid = yield ESelf()
    yield from mcast(overseer, (ENQUEUE, effect.fn_name, effect.args, bytes(me), effect.timeout_ms))
    envelope = yield EReceive()
    if envelope.body == _TIMEOUT_SENTINEL:
        raise JobTimeout(f"{effect.fn_name} timed out after {effect.timeout_ms}ms")
    if isinstance(envelope.body, JobError):
        raise envelope.body
    return envelope.body


def _handle_acquire(overseer: Pid, acquired: list[str], effect: EAcquire) -> Generator[Any, Any, bool]:
    result = yield from mcall(overseer, (ACQUIRE, effect.name, effect.limit))
    if result:
        acquired.append(effect.name)
    return result


def _handle_signal(overseer: Pid, effect: ESignal) -> Generator[Any, Any, str]:
    return (yield from mcall(overseer, (SIGNAL, effect.name)))


def _handle_set_semaphore(overseer: Pid, effect: ESetSemaphore) -> Generator[Any, Any, None]:
    yield from mcast(overseer, (SET_SEMAPHORE, effect.name, effect.state))


def make_handlers(overseer: Pid, acquired: list[str]) -> dict[type, Any]:
    return {
        ESatisfied:    _handle_event,
        EImpossible:   _handle_event,
        EAwait:        partial(_handle_await, overseer),
        EAcquire:      partial(_handle_acquire, overseer, acquired),
        ESignal:       partial(_handle_signal, overseer),
        ESetSemaphore: partial(_handle_set_semaphore, overseer),
    }
