"""Monotonic-clock deadline arithmetic, immune to wall-clock steps."""


def calculate_monotonic_deadline(current_time: float, timeout_ms: int) -> float:
    """Add a timeout in ms to a monotonic-clock instant.

    timeout_ms=0 is a real immediate deadline.
    """

    return current_time + timeout_ms / 1000
