from collections.abc import Generator
from typing import Never, TypedDict

from zahir.base_types import Context
from zahir.dependencies.time import TimeDependency
from zahir.events import Await, JobOutputEvent
from zahir.jobs.decorator import spec


class SleepOutput(TypedDict):
    duration_seconds: float


class SleepArgs(TypedDict):
    duration_seconds: float


def sleep_precheck(args):
    """Validate sleep job arguments before execution."""
    duration_seconds = args.get("duration_seconds", 0)

    if not isinstance(duration_seconds, (int, float)):
        return ValueError("duration_seconds must be a number")

    if duration_seconds < 0:
        return ValueError("duration_seconds must be non-negative")

    return None


@spec(args=SleepArgs, output=SleepOutput, precheck=sleep_precheck)
def Sleep(
    context: Context, args: SleepArgs, dependencies
) -> Generator[Await[TimeDependency, Never] | JobOutputEvent[SleepOutput]]:
    """A job that sleeps for a specified duration."""

    duration_seconds = args.get("duration_seconds", 0)

    yield Await(TimeDependency.seconds_from_now(duration_seconds))
    yield JobOutputEvent[SleepOutput](output={"duration_seconds": duration_seconds})
