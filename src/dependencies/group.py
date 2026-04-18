from collections.abc import Generator
from typing import Any

from effects import EImpossible, ESatisfied


type Dependency = Generator[Any, Any, None]


def group_dependency(
    dependencies: list[Dependency],
) -> Generator[Any, Any, None]:

    for dependency in dependencies:
        handler_value = None
        try:
            event = next(dependency)
            while True:
                handler_value = yield event
                if isinstance(event, EImpossible):
                    return
                event = dependency.send(handler_value)
        except StopIteration:
            pass
