"""Predicates and extractors for zahir telemetry events."""

from bookman.events import Event

from zahir.core.constants import JobTag, Phase


def has_fn(ev: Event) -> bool:
    """True for events emitted by an active worker executing a job.

    Idle workers emit EGetJob events with no fn dimension; active workers emit
    EEnqueue, EJobComplete, and EJobFail which all carry fn_name.
    """

    return bool(ev.dim("fn"))


def get_pid(ev: Event) -> str:
    """Extract the worker pid dimension from an event."""

    return ev.dim("pid")


def get_fn(ev: Event) -> str:
    """Extract the fn_name dimension from an event."""

    return ev.dim("fn")


def get_job_id(ev: Event) -> str:
    """Extract the job_id dimension from an event."""

    return ev.dim("job_id")


def is_enqueue_start(ev: Event) -> bool:
    """True when a job has been enqueued and is about to start."""

    return ev.dim("tag") == JobTag.ENQUEUE and ev.dim("phase") == Phase.START


def is_job_complete(ev: Event) -> bool:
    """True when a job has completed successfully."""

    return ev.dim("tag") == JobTag.JOB_COMPLETE and ev.dim("phase") == Phase.END


def is_job_fail(ev: Event) -> bool:
    """True when a job has failed."""

    return ev.dim("tag") == JobTag.JOB_FAIL and ev.dim("phase") == Phase.END


def is_job_lifecycle(ev: Event) -> bool:
    """True for job_lifecycle span events, which carry the full enqueue-to-completion duration."""

    return ev.dim("tag") == JobTag.JOB_LIFECYCLE


def get_duration_ms(ev: Event) -> float:
    """Extract event duration in milliseconds."""

    return ev.duration("ms")
