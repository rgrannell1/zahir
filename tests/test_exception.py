"""Tests for Zahir exceptions"""

from zahir.exception import ZahirException, DependencyMissingException


def test_exceptions_instantiate():
    """Test that all exception types can be instantiated."""
    ZahirException("test error")
    DependencyMissingException("missing dependency")
