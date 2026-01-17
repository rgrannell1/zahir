"""Tests for the ResourceLimit dependency."""

import time
from datetime import UTC, datetime, timedelta

import pytest

from zahir.base_types import DependencyState
from zahir.dependencies.resources import ResourceLimit, ResourceType


class TestResourceLimitBasic:
    """Basic functionality tests."""

    def test_cpu_satisfied_at_high_threshold(self):
        """CPU limit should be satisfied when threshold is high (90%)."""
        limit = ResourceLimit.cpu(max_percent=90.0)
        assert limit.satisfied() == DependencyState.SATISFIED

    def test_memory_satisfied_at_high_threshold(self):
        """Memory limit should be satisfied when threshold is high (90%)."""
        limit = ResourceLimit.memory(max_percent=90.0)
        assert limit.satisfied() == DependencyState.SATISFIED

    def test_cpu_unsatisfied_at_zero_threshold(self):
        """CPU limit should be unsatisfied when threshold is 0%."""
        limit = ResourceLimit.cpu(max_percent=0.0)
        assert limit.satisfied() == DependencyState.UNSATISFIED

    def test_memory_unsatisfied_at_zero_threshold(self):
        """Memory limit should be unsatisfied when threshold is 0%."""
        limit = ResourceLimit.memory(max_percent=0.0)
        assert limit.satisfied() == DependencyState.UNSATISFIED


class TestResourceLimitTimeout:
    """Timeout functionality tests."""

    def test_timeout_becomes_impossible(self):
        """Dependency should become IMPOSSIBLE after timeout."""
        limit = ResourceLimit.cpu(max_percent=0.0, timeout=0.1)

        # Before timeout - should be unsatisfied (not impossible)
        assert limit.satisfied() == DependencyState.UNSATISFIED

        # Wait for timeout
        time.sleep(0.15)

        # After timeout - should be impossible
        assert limit.satisfied() == DependencyState.IMPOSSIBLE

    def test_no_timeout_never_impossible(self):
        """Without timeout, dependency should never become IMPOSSIBLE."""
        limit = ResourceLimit.cpu(max_percent=0.0)
        assert limit.timeout_at is None
        # Even with impossible threshold, state is UNSATISFIED not IMPOSSIBLE
        assert limit.satisfied() == DependencyState.UNSATISFIED


class TestResourceLimitExtension:
    """Extension functionality tests."""

    def test_request_extension_extends_timeout(self):
        """request_extension should extend the timeout deadline."""
        limit = ResourceLimit.cpu(max_percent=80.0, timeout=10.0)
        original_timeout = limit.timeout_at

        extended = limit.request_extension(5.0)

        assert extended.timeout_at is not None
        assert original_timeout is not None
        assert extended.timeout_at == original_timeout + timedelta(seconds=5.0)

    def test_request_extension_without_timeout_returns_self(self):
        """request_extension without timeout should return self unchanged."""
        limit = ResourceLimit.cpu(max_percent=80.0)
        extended = limit.request_extension(5.0)
        assert extended is limit

    def test_request_extension_preserves_resource_and_max_percent(self):
        """Extension should preserve resource type and max_percent."""
        limit = ResourceLimit.memory(max_percent=75.0, timeout=10.0)
        extended = limit.request_extension(5.0)

        assert extended.resource == ResourceType.MEMORY
        assert extended.max_percent == 75.0


class TestResourceLimitSerialization:
    """Serialization and deserialization tests."""

    def test_save_without_timeout(self):
        """Save should produce correct dict without timeout."""
        limit = ResourceLimit.cpu(max_percent=80.0)
        data = limit.save(None)

        assert data["type"] == "ResourceLimit"
        assert data["resource"] == "cpu"
        assert data["max_percent"] == 80.0
        assert data["timeout_at"] is None

    def test_save_with_timeout(self):
        """Save should produce correct dict with timeout."""
        limit = ResourceLimit.memory(max_percent=70.0, timeout=60.0)
        data = limit.save(None)

        assert data["type"] == "ResourceLimit"
        assert data["resource"] == "memory"
        assert data["max_percent"] == 70.0
        assert data["timeout_at"] is not None

    def test_load_without_timeout(self):
        """Load should reconstruct ResourceLimit without timeout."""
        data = {
            "type": "ResourceLimit",
            "resource": "cpu",
            "max_percent": 85.0,
            "timeout_at": None,
        }

        loaded = ResourceLimit.load(None, data)

        assert loaded.resource == ResourceType.CPU
        assert loaded.max_percent == 85.0
        assert loaded.timeout_at is None

    def test_load_with_timeout(self):
        """Load should reconstruct ResourceLimit with timeout."""
        timeout_at = datetime.now(tz=UTC) + timedelta(seconds=300)
        data = {
            "type": "ResourceLimit",
            "resource": "memory",
            "max_percent": 75.0,
            "timeout_at": timeout_at.isoformat(),
        }

        loaded = ResourceLimit.load(None, data)

        assert loaded.resource == ResourceType.MEMORY
        assert loaded.max_percent == 75.0
        assert loaded.timeout_at == timeout_at

    def test_roundtrip_serialization(self):
        """Save then load should produce equivalent ResourceLimit."""
        original = ResourceLimit.memory(max_percent=65.0, timeout=120.0)
        data = original.save(None)
        loaded = ResourceLimit.load(None, data)

        assert loaded.resource == original.resource
        assert loaded.max_percent == original.max_percent
        assert loaded.timeout_at == original.timeout_at


class TestResourceType:
    """ResourceType enum tests."""

    def test_cpu_value(self):
        assert ResourceType.CPU.value == "cpu"

    def test_memory_value(self):
        assert ResourceType.MEMORY.value == "memory"

    def test_from_string(self):
        assert ResourceType("cpu") == ResourceType.CPU
        assert ResourceType("memory") == ResourceType.MEMORY
