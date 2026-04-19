from collections.abc import Generator
from functools import partial
from typing import Any

from tertius import EReceive, ESelf, Pid, mcall, mcast

from constants import ACQUIRE, ENQUEUE, RELEASE, SET_SEMAPHORE, SIGNAL
from effects import EAcquire, EAwait, EAwaitAll, EImpossible, ESatisfied, ESetSemaphore, ESignal
from exceptions import JobError, JobTimeout


_TIMEOUT_SENTINEL = "__timeout__"


def _handle_event(effect: ESatisfied | EImpossible) -> Generator[Any, Any, ESatisfied | EImpossible]:
    return effect
    yield


def _unwrap_reply(fn_name: str, timeout_ms: int | None, nonce: Any, body: Any) -> Any:
    if body == _TIMEOUT_SENTINEL:
        raise JobTimeout(f"{fn_name} timed out after {timeout_ms}ms")

    if isinstance(body, JobError):
        raise body

    return body


def _handle_await(overseer: Pid, effect: EAwait) -> Generator[Any, Any, Any]:
    me: Pid = yield ESelf()
    yield from mcast(overseer, (ENQUEUE, effect.fn_name, effect.args, bytes(me), effect.timeout_ms, None))
    envelope = yield EReceive()
    nonce, body = envelope.body
    return _unwrap_reply(effect.fn_name, effect.timeout_ms, nonce, body)


def _handle_await_all(overseer: Pid, effect: EAwaitAll) -> Generator[Any, Any, list[Any]]:
    me: Pid = yield ESelf()

    for idx, eff in enumerate(effect.effects):
        yield from mcast(overseer, (ENQUEUE, eff.fn_name, eff.args, bytes(me), eff.timeout_ms, idx))

    results: list[Any] = [None] * len(effect.effects)
    first_error: Exception | None = None

    for _ in effect.effects:
        envelope = yield EReceive()
        nonce, body = envelope.body
        try:
            fn_name = effect.effects[nonce].fn_name
            results[nonce] = _unwrap_reply(fn_name, effect.effects[nonce].timeout_ms, nonce, body)
        except (JobTimeout, JobError) as exc:
            # let's throw the first error we see.
            if first_error is None:
                first_error = exc

    if first_error is not None:
        raise first_error

    return results


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
        EAwaitAll:     partial(_handle_await_all, overseer),
        EAcquire:      partial(_handle_acquire, overseer, acquired),
        ESignal:       partial(_handle_signal, overseer),
        ESetSemaphore: partial(_handle_set_semaphore, overseer),
    }
