"""Retry combinator: re-awaits a failed scalar job with linear backoff."""

from collections.abc import Generator
from typing import Any

from tertius import EEmit, ESleep

from zahir.core.effects import EAwait
from zahir.core.emit import retry_event
from zahir.core.exceptions import JobError, JobTimeoutError


def retried(spec: EAwait, attempts: int, backoff_ms: int) -> Generator[Any, Any, Any]:
    """Await a scalar job, retrying failed attempts with linear backoff.

    Re-yields the same EAwait up to attempts times. Each failure short of the
    last emits a retry telemetry event and sleeps backoff_ms * attempt before
    re-dispatching; the final failure propagates to the caller unchanged.

    Note the backoff sleep pauses the awaiting job in place — it does not free
    the worker the way EAwait suspension does, so keep backoffs short.

    Use via: result = yield from retried(ctx.scope.fetch(url), attempts=3, backoff_ms=500)
    """

    if not spec.scalar:
        raise ValueError("retried only accepts scalar EAwaits — wrap each job separately")
    if attempts < 1:
        raise ValueError("attempts must be at least 1")

    fn_name = spec.jobs[0].fn_name

    for attempt in range(1, attempts + 1):
        try:
            return (yield spec)  # noqa: B901
        except (JobError, JobTimeoutError) as err:
            if attempt == attempts:
                raise
            yield EEmit(retry_event(fn_name, attempt, err))
            yield ESleep(ms=backoff_ms * attempt)
