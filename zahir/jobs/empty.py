from collections.abc import Generator

from zahir.base_types import Context
from zahir.dependencies.group import DependencyGroup
from zahir.jobs.decorator import spec


@spec()
def Empty(context: Context, args: None, dependencies: DependencyGroup) -> Generator[None]:
    """A job that does nothing. Useful if you need to block on a dependency, but not do anything when it's satisfied."""
    yield from []
