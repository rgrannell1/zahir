"""Tests for Zahir exceptions"""

from zahir.exception import DependencyMissingException, ZahirException


def test_exceptions_instantiate():
    """Test that all exception types can be instantiated."""
    ZahirException("test error")
    DependencyMissingException("missing dependency")
