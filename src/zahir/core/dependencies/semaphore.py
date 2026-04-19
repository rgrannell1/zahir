from collections.abc import Generator
from datetime import UTC, datetime, timedelta

from tertius import ESleep

from zahir.core.constants import DEPENDENCY_DELAY_MS, IMPOSSIBLE, SATISFIED, UNSATISFIED
from zahir.core.effects import EImpossible, ESatisfied, EGetSemaphore


def semaphore_dependency(
    name: str,
    timeout_ms: int | None = None,
) -> Generator[EGetSemaphore | ESleep | ESatisfied | EImpossible, str | None, ESatisfied | EImpossible]:
    timeout_at = (
        datetime.now(tz=UTC) + timedelta(milliseconds=timeout_ms)
        if timeout_ms is not None
        else None
    )

    while True:
        if timeout_at is not None and datetime.now(tz=UTC) >= timeout_at:
            event = EImpossible(
                reason=f"semaphore '{name}' not satisfied within {timeout_ms}ms"
            )
            yield event
            return event

        state = yield EGetSemaphore(name=name)

        if state == SATISFIED:
            event = ESatisfied(metadata={"name": name})
            yield event
            return event
        elif state == IMPOSSIBLE:
            event = EImpossible(reason=f"semaphore '{name}' aborted")
            yield event
            return event
        elif state == UNSATISFIED:
            yield ESleep(ms=DEPENDENCY_DELAY_MS)
