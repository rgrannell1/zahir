from collections.abc import Generator
from datetime import UTC, datetime, timedelta

from tertius import ESleep

from constants import DEPENDENCY_DELAY_MS
from effects import EAcquire, EImpossible, ESatisfied


def concurrency_dependency(
    name: str,
    limit: int,
    timeout_ms: int | None = None,
) -> Generator[EAcquire | ESleep | ESatisfied | EImpossible, bool | None, None]:
    timeout_at = (
        datetime.now(tz=UTC) + timedelta(milliseconds=timeout_ms)
        if timeout_ms is not None
        else None
    )

    while True:
        if timeout_at is not None and datetime.now(tz=UTC) >= timeout_at:
            event = EImpossible(
                reason=f"concurrency slot '{name}' not available within {timeout_ms}ms"
            )
            yield event
            return event

        acquired: bool = yield EAcquire(name=name, limit=limit)

        if acquired:
            event = ESatisfied(metadata={"name": name, "limit": limit})
            yield event
            return event

        yield ESleep(ms=DEPENDENCY_DELAY_MS)
