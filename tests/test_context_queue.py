"""Tests for MemoryContext queue functionality."""

import multiprocessing

from zahir.context import MemoryContext
from zahir.job_registry import SQLiteJobRegistry
from zahir.scope import LocalScope


def test_add_queue_returns_id_and_queue():
    """Test that add_queue returns a unique ID and a queue."""
    scope = LocalScope()
    registry = SQLiteJobRegistry(":memory:")
    registry.init("test-worker")
    context = MemoryContext(scope=scope, job_registry=registry)

    queue_id, queue = context.add_queue()

    assert isinstance(queue_id, str)
    assert len(queue_id) > 0
    assert queue is not None


def test_get_queue_retrieves_by_id():
    """Test that get_queue retrieves the queue by ID."""
    scope = LocalScope()
    registry = SQLiteJobRegistry(":memory:")
    registry.init("test-worker")
    context = MemoryContext(scope=scope, job_registry=registry)

    queue_id, queue = context.add_queue()

    # Get the queue by ID
    retrieved_queue = context.get_queue(queue_id)

    # Should be able to use it
    assert retrieved_queue is not None


def test_add_queue_creates_multiple_queues():
    """Test that add_queue can create multiple distinct queues."""
    scope = LocalScope()
    registry = SQLiteJobRegistry(":memory:")
    registry.init("test-worker")
    context = MemoryContext(scope=scope, job_registry=registry)

    queue_id1, queue1 = context.add_queue()
    queue_id2, queue2 = context.add_queue()

    # IDs should be different
    assert queue_id1 != queue_id2

    # Both should be retrievable
    retrieved1 = context.get_queue(queue_id1)
    retrieved2 = context.get_queue(queue_id2)

    assert retrieved1 is not None
    assert retrieved2 is not None


def test_get_queue_raises_keyerror_for_nonexistent():
    """Test that get_queue raises KeyError for non-existent queue."""
    scope = LocalScope()
    registry = SQLiteJobRegistry(":memory:")
    registry.init("test-worker")
    context = MemoryContext(scope=scope, job_registry=registry)

    try:
        context.get_queue("non-existent-id")
        assert False, "Should have raised KeyError"
    except KeyError as err:
        assert "non-existent-id" in str(err)


def test_queue_stored_in_context_state():
    """Test that queue is stored in context.state."""
    scope = LocalScope()
    registry = SQLiteJobRegistry(":memory:")
    registry.init("test-worker")
    context = MemoryContext(scope=scope, job_registry=registry)

    queue_id, queue = context.add_queue()

    # Check it's in state with the right key
    key = f"_queue_{queue_id}"
    assert key in context.state


def test_queue_operations_work():
    """Test that basic queue operations work."""
    scope = LocalScope()
    registry = SQLiteJobRegistry(":memory:")
    registry.init("test-worker")
    context = MemoryContext(scope=scope, job_registry=registry)

    queue_id, queue = context.add_queue()

    # Put something on the queue
    test_value = {"test": "data"}
    queue.put(test_value)

    # Get it back
    retrieved_value = queue.get(timeout=1)
    assert retrieved_value == test_value


def test_queue_from_get_queue_operations_work():
    """Test that operations on queue retrieved via get_queue work."""
    scope = LocalScope()
    registry = SQLiteJobRegistry(":memory:")
    registry.init("test-worker")
    context = MemoryContext(scope=scope, job_registry=registry)

    queue_id, queue = context.add_queue()

    # Put something on the queue using add_queue's queue
    test_value = {"test": "data"}
    queue.put(test_value)

    # Get it back using get_queue's queue
    retrieved_queue = context.get_queue(queue_id)
    retrieved_value = retrieved_queue.get(timeout=1)
    assert retrieved_value == test_value
