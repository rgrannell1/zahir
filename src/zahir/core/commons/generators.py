"""Generator resume plumbing shared by the guard stack and the worker loop."""

from collections.abc import Generator
from typing import Any


def resume(
    gen: Generator,
    send_value: Any,
    pending_throw: Exception | None,
) -> tuple[Any, bool, Any]:
    """Resume gen with a throw or a send; return (effect, done, return_value)."""

    try:
        effect = gen.throw(pending_throw) if pending_throw else gen.send(send_value)
        return (effect, False, None)
    except StopIteration as stop:
        return (None, True, stop.value)
