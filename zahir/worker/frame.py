
from collections.abc import Generator
from dataclasses import dataclass, field

from zahir.base_types import Job


@dataclass
class ZahirCallStack:
    """Represents this worker process's call-stack of jobs"""

    frames: list["ZahirStackFrame"]

    def push(self, frame: "ZahirStackFrame"):
        self.frames.append(frame)

    def pop(self) -> "ZahirStackFrame":
        if not self.frames:
            raise IndexError("pop from empty call stack")
        return self.frames.pop()

    def is_empty(self) -> bool:
        return len(self.frames) == 0

    def top(self) -> "ZahirStackFrame | None":
        if not self.frames:
            return None
        return self.frames[-1]


@dataclass
class ZahirStackFrame:
    """An instantiated job, its run or recovery generator, and whether it's awaited.

    Zahir job workers maintain a stack of call-frames for nested job execution.
    """

    job: Job
    job_generator: Generator
    recovery: bool = False
    required_jobs: set[str] = field(default_factory=set)

    def job_type(self) -> str:
        return type(self.job).__name__
