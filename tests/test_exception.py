"""Tests for Zahir exceptions"""

from zahir.exception import DependencyMissingError, ZahirError


def test_exceptions_instantiate():
    """Test that all exception types can be instantiated."""
    ZahirError("test error")
    DependencyMissingError("missing dependency")
