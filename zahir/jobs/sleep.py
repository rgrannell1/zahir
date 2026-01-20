from collections.abc import Generator
from dataclasses import dataclass
from typing import TypedDict

from zahir.base_types import Context, JobInstance
from zahir.dependencies.time import TimeDependency
from zahir.events import JobOutputEvent
from zahir.jobs import Empty
from zahir.jobs.decorator import spec


def _sleep_precheck(args):
    """Validate sleep job arguments before execution."""
    duration_seconds = args.get("duration_seconds", 0)

    if not isinstance(duration_seconds, (int, float)):
        return ValueError("duration_seconds must be a number")

    if duration_seconds < 0:
        return ValueError("duration_seconds must be non-negative")

    return None

@dataclass
class SleepOutput(TypedDict):
    duration_seconds: float

@dataclass
class SleepArgs(TypedDict):
    duration_seconds: float

@spec(precheck=_sleep_precheck)
def Sleep(context: Context, args: SleepArgs, dependencies) -> Generator[JobInstance | JobOutputEvent[SleepOutput]]:
    """A job that sleeps for a specified duration."""
    duration_seconds = args.get("duration_seconds", 0)

    yield Empty[None, None]({}, {"wait_seconds": TimeDependency.seconds_from_now(duration_seconds)})
    yield JobOutputEvent[SleepOutput](output={"duration_seconds": duration_seconds})
