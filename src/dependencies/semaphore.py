from collections.abc import Generator
from datetime import UTC, datetime, timedelta

from tertius import ESleep

from constants import DEPENDENCY_DELAY_MS
from effects import EImpossible, ESatisfied, ESignal


def semaphore_dependency(
    name: str,
    timeout_ms: int | None = None,
) -> Generator[ESignal | ESleep | ESatisfied | EImpossible, str | None, None]:
    timeout_at = (
        datetime.now(tz=UTC) + timedelta(milliseconds=timeout_ms)
        if timeout_ms is not None
        else None
    )

    while True:
        if timeout_at is not None and datetime.now(tz=UTC) >= timeout_at:
            event = EImpossible(reason=f"semaphore '{name}' not satisfied within {timeout_ms}ms")
            yield event
            return event

        state: str = yield ESignal(name=name)

        match state:
            case "satisfied":
                event = ESatisfied(metadata={"name": name})
                yield event
                return event
            case "impossible":
                event = EImpossible(reason=f"semaphore '{name}' aborted")
                yield event
                return event
            case "unsatisfied":
                yield ESleep(ms=DEPENDENCY_DELAY_MS)
