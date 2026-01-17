from collections.abc import Generator
import time

from zahir.base_types import Context
from zahir.events import JobOutputEvent
from zahir.jobs.decorator import spec


def _sleep_precheck(spec_args, args):
    """Validate sleep job arguments before execution."""
    duration_seconds = args.get("duration_seconds", 0)

    if not isinstance(duration_seconds, (int, float)):
        return ValueError("duration_seconds must be a number")

    if duration_seconds < 0:
        return ValueError("duration_seconds must be non-negative")

    return None


@spec(precheck=_sleep_precheck)
def Sleep(spec_args, context: Context, args, dependencies) -> Generator[JobOutputEvent]:
    """A job that sleeps for a specified duration."""
    duration_seconds = args.get("duration_seconds", 0)

    if not isinstance(duration_seconds, (int, float)) or duration_seconds < 0:
        raise ValueError("duration_seconds must be a non-negative number")

    # Not a good implementation, but ok for now.
    time.sleep(duration_seconds)
    yield JobOutputEvent(output={"duration_seconds": duration_seconds})
