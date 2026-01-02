"""Tests for Zahir exceptions"""

from zahir.exception import (
    DependencyMissingError,
    DependencyNotInScopeError,
    DuplicateJobError,
    ImpossibleDependencyError,
    JobNotInScopeError,
    JobPrecheckError,
    JobRecoveryTimeoutError,
    JobTimeoutError,
    MissingJobError,
    NotInScopeError,
    ZahirError,
    ZahirInternalError,
)


def test_exceptions_instantiate():
    """Test that all exception types can be instantiated."""
    ZahirError("test error")
    ZahirInternalError("internal error")
    DuplicateJobError("duplicate job")
    MissingJobError("job not found")
    JobPrecheckError("precheck failed")
    ImpossibleDependencyError("impossible dependency")
    DependencyMissingError("missing dependency")
    JobTimeoutError("job timed out")
    JobRecoveryTimeoutError("recovery timed out")
    NotInScopeError("not in scope")
    JobNotInScopeError("job not in scope")
    DependencyNotInScopeError("dependency not in scope")
