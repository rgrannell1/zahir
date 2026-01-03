"""Tests for zahir.worker.job_worker module."""

import multiprocessing
import tempfile
from unittest.mock import Mock, patch

import pytest

from zahir.context import MemoryContext
from zahir.job_registry import SQLiteJobRegistry
from zahir.scope import LocalScope
from zahir.worker.job_worker import zahir_job_worker
from zahir.worker.state_machine.states import (
    ExecuteJobStateChange,
    PopJobStateChange,
)


def test_job_worker_basic_flow():
    """Test that job_worker initializes registry and starts state machine."""
    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    job_registry = SQLiteJobRegistry(tmp_file)

    context = MemoryContext(
        scope=LocalScope(jobs=[]),
        job_registry=job_registry,
    )
    output_queue = multiprocessing.Queue()
    workflow_id = "test-workflow-1"

    # Mock the state machine to control flow
    mock_handler = Mock()
    mock_handler.return_value = (PopJobStateChange({}), Mock())

    with patch(
        "zahir.worker.job_worker.ZahirJobStateMachine.get_state",
        return_value=mock_handler,
    ):
        # Run one iteration by making handler raise exception to exit
        def side_effect_exit(*args):
            if mock_handler.call_count > 1:
                raise KeyboardInterrupt("Test exit")
            return (PopJobStateChange({}), Mock())

        mock_handler.side_effect = side_effect_exit

        with pytest.raises(KeyboardInterrupt):
            zahir_job_worker(context, output_queue, workflow_id)

    # Verify handler was called
    assert mock_handler.call_count > 0


def test_job_worker_handles_state_machine_exception():
    """Test that job_worker handles exceptions from state machine handlers by exiting gracefully."""
    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    job_registry = SQLiteJobRegistry(tmp_file)

    context = MemoryContext(
        scope=LocalScope(jobs=[]),
        job_registry=job_registry,
    )
    output_queue = multiprocessing.Queue()
    workflow_id = "test-workflow-1"

    # Mock the state machine to raise an exception
    mock_handler = Mock(side_effect=RuntimeError("Test error"))

    with patch(
        "zahir.worker.job_worker.ZahirJobStateMachine.get_state",
        return_value=mock_handler,
    ):
        # Should exit without raising (exception is caught internally)
        zahir_job_worker(context, output_queue, workflow_id)

    # Verify handler was called
    assert mock_handler.call_count == 1


def test_job_worker_handles_handler_returning_none():
    """Test that job_worker raises RuntimeError if handler returns None."""
    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    job_registry = SQLiteJobRegistry(tmp_file)

    context = MemoryContext(
        scope=LocalScope(jobs=[]),
        job_registry=job_registry,
    )
    output_queue = multiprocessing.Queue()
    workflow_id = "test-workflow-1"

    # Mock the state machine to return None
    mock_handler = Mock(return_value=None)

    with (
        patch(
            "zahir.worker.job_worker.ZahirJobStateMachine.get_state",
            return_value=mock_handler,
        ),
        pytest.raises(RuntimeError, match="returned None unexpectedly"),
    ):
        zahir_job_worker(context, output_queue, workflow_id)


def test_job_worker_initializes_registry():
    """Test that job_worker initializes the job registry with PID."""
    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    job_registry = SQLiteJobRegistry(tmp_file)

    context = MemoryContext(
        scope=LocalScope(jobs=[]),
        job_registry=job_registry,
    )
    output_queue = multiprocessing.Queue()
    workflow_id = "test-workflow-1"

    # Track init call
    original_init = job_registry.init
    init_called_with = []

    def track_init(pid):
        init_called_with.append(pid)
        return original_init(pid)

    job_registry.init = track_init

    # Mock state machine to exit quickly
    mock_handler = Mock(side_effect=KeyboardInterrupt("Exit"))

    with (
        patch(
            "zahir.worker.job_worker.ZahirJobStateMachine.get_state",
            return_value=mock_handler,
        ),
        pytest.raises(KeyboardInterrupt),
    ):
        zahir_job_worker(context, output_queue, workflow_id)

    # Verify init was called with a PID string
    assert len(init_called_with) == 1
    assert init_called_with[0].isdigit()


def test_job_worker_state_transitions():
    """Test that job_worker properly transitions between states."""
    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    job_registry = SQLiteJobRegistry(tmp_file)

    context = MemoryContext(
        scope=LocalScope(jobs=[]),
        job_registry=job_registry,
    )
    output_queue = multiprocessing.Queue()
    workflow_id = "test-workflow-1"

    # Track state transitions
    states_seen = []

    def mock_get_state(state_enum):
        states_seen.append(state_enum.value)

        # Create mock handler that returns next state
        if len(states_seen) == 1:
            # Start -> PopJob
            handler = Mock(return_value=(PopJobStateChange({}), Mock()))
        elif len(states_seen) == 2:
            # PopJob -> ExecuteJob
            handler = Mock(return_value=(ExecuteJobStateChange({"job_id": "test"}), Mock()))
        else:
            # Exit
            handler = Mock(side_effect=KeyboardInterrupt("Exit"))

        return handler

    with (
        patch(
            "zahir.worker.job_worker.ZahirJobStateMachine.get_state",
            side_effect=mock_get_state,
        ),
        pytest.raises(KeyboardInterrupt),
    ):
        zahir_job_worker(context, output_queue, workflow_id)

    # Verify we saw the expected state transitions
    assert "start" in states_seen
    assert "pop_job" in states_seen
    assert len(states_seen) >= 2
