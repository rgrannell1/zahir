
from zahir.jobs.decorator import spec


@spec()
def Empty(spec_args, context, args, dependencies):
    """A job that does nothing. Useful if you need to block on a dependency, but not do anything"""
    yield from []
