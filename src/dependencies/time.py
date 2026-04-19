from collections.abc import Generator
from datetime import UTC, datetime

from tertius import ESleep

from effects import EImpossible, ESatisfied


def time_dependency(
    before: datetime | None = None,
    after: datetime | None = None,
) -> Generator[ESleep | ESatisfied | EImpossible, None, ESatisfied | EImpossible]:
    now = datetime.now(tz=UTC)

    if before is not None and now >= before:
        event = EImpossible(
            reason=f"too late: now={now.isoformat()}, before={before.isoformat()}"
        )
        yield event
        return event

    if after is not None and now < after:
        ms = int((after - now).total_seconds() * 1000)
        yield ESleep(ms=ms)

    event = ESatisfied(
        metadata={
            "before": before.isoformat() if before else None,
            "after": after.isoformat() if after else None,
        }
    )
    yield event
    return event
