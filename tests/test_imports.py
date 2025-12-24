"""Test that all Python modules can be imported without circular dependency errors."""

import pytest


def test_imports():
    """Test importing all zahir modules."""
    import zahir
    import zahir.types
    import zahir.events
    import zahir.exception
    import zahir.context
    import zahir.scope
    import zahir.workflow
    import zahir.dependencies
    import zahir.dependencies.concurrency
    import zahir.dependencies.group
    import zahir.dependencies.job
    import zahir.dependencies.time
    import zahir.registries
    import zahir.registries.local
    import zahir.registries.sqlite
    import zahir.tasks.retry
