# Combinator that runs a sequence of dependencies and short-circuits on the first impossible result.
from collections.abc import Generator
from typing import Any

from tertius import EEmit

from zahir.core.constants import DependencyState as SS
from zahir.core.zahir_types import DependencyResult


def group_dependency(
    dependencies: list[Generator],
) -> Generator[Any, Any, DependencyResult]:
    """Run dependencies in sequence; short-circuit on the first impossible result."""

    if not dependencies:
        result: DependencyResult = (SS.SATISFIED, None)
        yield EEmit(result)
        return result

    last: DependencyResult | None = None
    for dep in dependencies:
        last = yield from dep
        if last[0] == SS.IMPOSSIBLE:
            return last

    assert last is not None
    return last
