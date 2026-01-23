"""Quick test to verify the calls feature works."""

from zahir.jobs.decorator import spec
from zahir.base_types import JobSpec, Context
from zahir.dependencies.group import DependencyGroup


@spec()
def JobA(context: Context, input, dependencies: DependencyGroup):
    """A simple job."""
    pass


@spec()
def JobB(context: Context, input, dependencies: DependencyGroup):
    """Another simple job."""
    pass


@spec(calls=[JobA, JobB])
def JobC(context: Context, input, dependencies: DependencyGroup):
    """A job that calls JobA and JobB."""
    pass


def test_calls_property():
    """Test that the calls property is set correctly."""
    assert isinstance(JobA, JobSpec)
    assert isinstance(JobB, JobSpec)
    assert isinstance(JobC, JobSpec)
    
    # JobA and JobB should have empty calls
    assert JobA.calls == []
    assert JobB.calls == []
    
    # JobC should have JobA and JobB in its calls
    assert len(JobC.calls) == 2
    assert JobA in JobC.calls
    assert JobB in JobC.calls
    
    print("✓ All tests passed!")


if __name__ == "__main__":
    test_calls_property()
