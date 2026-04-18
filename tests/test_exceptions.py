import pytest

from exceptions import JobTimeout, ZahirException


def test_zahir_exception_is_exception():
    """Proves ZahirException is a subclass of Exception."""

    assert issubclass(ZahirException, Exception)


def test_job_timeout_is_zahir_exception():
    """Proves JobTimeout is a subclass of ZahirException."""

    assert issubclass(JobTimeout, ZahirException)


def test_job_timeout_is_catchable_as_zahir_exception():
    """Proves JobTimeout can be caught via ZahirException."""

    with pytest.raises(ZahirException):
        raise JobTimeout()


def test_job_timeout_is_catchable_as_exception():
    """Proves JobTimeout can be caught via the base Exception type."""

    with pytest.raises(Exception):
        raise JobTimeout()


def test_job_timeout_accepts_message():
    """Proves JobTimeout preserves a message string."""

    exc = JobTimeout("timed out after 5000ms")
    assert "5000ms" in str(exc)
