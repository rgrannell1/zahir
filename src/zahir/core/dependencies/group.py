from collections.abc import Generator
from typing import Any

from zahir.core.effects import EImpossible, ESatisfied


def group_dependency(
    dependencies: list[Generator],
) -> Generator[Any, Any, ESatisfied | EImpossible]:
    """Run dependencies in sequence; short-circuit on the first impossible result."""
    if not dependencies:
        event = ESatisfied()
        yield event
        return event

    last = None
    for dep in dependencies:
        last = yield from dep
        if isinstance(last, EImpossible):
            return last
    return last
