"""Monotonic-clock deadline arithmetic, immune to wall-clock steps."""

import time


def monotonic_deadline(timeout_ms: int | None) -> float | None:
    """Convert a timeout in ms to a monotonic-clock instant.

    timeout_ms=0 is a real (immediate) deadline; only None means no deadline.
    """

    if timeout_ms is None:
        return None
    return time.monotonic() + timeout_ms / 1000
