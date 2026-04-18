from collections.abc import Generator
from datetime import UTC, datetime

from tertius import ESleep

from effects import EImpossible, ESatisfied


def time_dependency(
    before: datetime | None,
    after: datetime | None,
) -> Generator[ESleep | ESatisfied | EImpossible, None, None]:
    now = datetime.now(tz=UTC)

    if before is not None and now >= before:
        yield EImpossible(reason=f"too late: now={now.isoformat()}, before={before.isoformat()}")
        return

    if after is not None and now < after:
        ms = int((after - now).total_seconds() * 1000)
        yield ESleep(ms=ms)

    yield ESatisfied(metadata={
        "before": before.isoformat() if before else None,
        "after": after.isoformat() if after else None,
    })
