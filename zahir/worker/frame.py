
from collections.abc import Generator
from dataclasses import dataclass, field

from zahir.base_types import Job, JobRegistry, JobState
from zahir.exception import ZahirInternalError

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

    def runnable_frame_idx(self, job_registry: JobRegistry) -> int | None:
        """Can we actually run any of these jobs right now? If we can, return
        the index of the bottom-most runnable frame (FIFO). We can run jobs if:

        - They aren't awaiting any other jobs. Only `Paused` or `Running` jobs should be on the stack.
        - The jobs is awaiting jobs in a completed state.
        """

        idx = len(self.frames) - 1

        for frame in reversed(self.frames):
            job = frame.job
            job_state = job_registry.get_state(job.job_id)

            # TO-DO: check call stack only maintained Ready and Pending jobs

            # This job is `Paused`` and awaiting other jobs
            if frame.required_jobs:
                # All frames are complete. This does not mean the job is healthy.
                all_done = all(
                    job_registry.is_finished(required_id)
                    for required_id in frame.required_jobs
                )
                return idx if all_done else None
            else:
                # This Job should be running, I don't think this case can occur.
                return idx

            idx -= 1

        raise NotImplementedError


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
