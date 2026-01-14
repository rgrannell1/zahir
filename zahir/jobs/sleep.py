from collections.abc import Iterator
import time

from zahir.base_types import Job
from zahir.events import JobOutputEvent


class Sleep(Job):
    """
    A job that sleeps for a specified duration.
    """

    @staticmethod
    def precheck(input):
        duration_seconds = input.get("duration_seconds")

        if duration_seconds is not None and (not isinstance(duration_seconds, (int, float)) or duration_seconds < 0):
            return ValueError("duration_seconds must be a non-negative number")

        return None

    @classmethod
    def run(cls, context, input, dependencies) -> Iterator["JobOutputEvent"]:  # noqa: ARG003
        duration_seconds = input.get("duration_seconds")

        # Not a good implementation, but ok for now.
        time.sleep(duration_seconds)
        yield JobOutputEvent(output={"duration_seconds": duration_seconds})
