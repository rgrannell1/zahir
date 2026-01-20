from decimal import Context
from typing import Generator
from zahir.dependencies.group import DependencyGroup
from zahir.jobs.decorator import spec


@spec()
def Empty(context: Context, args: None, dependencies: DependencyGroup) -> Generator[None, None, None]:
    """A job that does nothing. Useful if you need to block on a dependency, but not do anything when it's satisfied."""
    yield from []
