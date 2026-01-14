"""Pytest configuration and fixtures"""

import tempfile

import pytest

from zahir.context import MemoryContext
from zahir.job_registry import SQLiteJobRegistry
from zahir.scope import LocalScope


@pytest.fixture
def simple_context():
    """Provide a minimal context for tests that just need to pass context to methods.

    This is useful for event serialization tests that don't actually use the context.
    """
    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    job_registry = SQLiteJobRegistry(tmp_file)
    job_registry.init("test-worker")

    context = MemoryContext(scope=LocalScope(jobs=[]), job_registry=job_registry)
    return context
