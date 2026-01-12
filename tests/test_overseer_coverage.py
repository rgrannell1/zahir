"""Tests to improve code coverage for worker/overseer.py

Focusing on uncovered lines:
- Line 115: isinstance check for ZahirInternalErrorEvent with event.error
- Line 120: RuntimeError when event.error is None
- Line 130-131: KeyboardInterrupt exception handling
- Line 136: exc is not None raise
"""

from collections.abc import Iterator
import multiprocessing
from unittest.mock import MagicMock, patch

import pytest

from zahir.base_types import Context, Job
from zahir.context.memory import MemoryContext
from zahir.dependencies.group import DependencyGroup
from zahir.events import (
    JobOutputEvent,
    WorkflowCompleteEvent,
    WorkflowOutputEvent,
    WorkflowStartedEvent,
    ZahirCustomEvent,
    ZahirInternalErrorEvent,
)
from zahir.exception import exception_to_text_blob
from zahir.job_registry import SQLiteJobRegistry
from zahir.scope import LocalScope
from zahir.worker.overseer import shutdown, zahir_worker_overseer


class SimpleOutputJob(Job):
    """A simple job that outputs a result."""

    @classmethod
    def run(
        cls,
        context: Context,
        input: dict,
        dependencies: DependencyGroup,
    ) -> Iterator[Job | JobOutputEvent]:
        yield JobOutputEvent(output={"result": "success"})


def test_shutdown_terminates_alive_processes():
    """Test that shutdown() terminates alive processes."""
    # Create mock processes
    proc1 = MagicMock(spec=multiprocessing.Process)
    proc1.is_alive.return_value = True

    proc2 = MagicMock(spec=multiprocessing.Process)
    proc2.is_alive.return_value = False

    processes = [proc1, proc2]

    shutdown(processes)

    # proc1 should be terminated since it's alive
    proc1.terminate.assert_called_once()
    proc1.join.assert_called_once_with(timeout=5)

    # proc2 should not be terminated since it's not alive
    proc2.terminate.assert_not_called()
    proc2.join.assert_called_once_with(timeout=5)


def test_shutdown_joins_all_processes():
    """Test that shutdown() joins all processes."""
    proc1 = MagicMock(spec=multiprocessing.Process)
    proc1.is_alive.return_value = True

    proc2 = MagicMock(spec=multiprocessing.Process)
    proc2.is_alive.return_value = True

    processes = [proc1, proc2]

    shutdown(processes)

    # Both should be joined
    proc1.join.assert_called_once_with(timeout=5)
    proc2.join.assert_called_once_with(timeout=5)


def test_overseer_internal_error_without_error_message():
    """Test overseer handling ZahirInternalErrorEvent without error message (line 120)."""
    scope = LocalScope()
    scope.add_job_class(SimpleOutputJob)
    job_registry = SQLiteJobRegistry(":memory:")
    context = MemoryContext(scope=scope, job_registry=job_registry)

    # Create a job
    job = SimpleOutputJob(input={}, dependencies={})

    # We'll mock the output queue to inject an internal error event
    with patch("zahir.worker.overseer.multiprocessing.Queue") as mock_queue_class:
        mock_queue = MagicMock()
        mock_queue_class.return_value = mock_queue

        # Set up the queue to return events in sequence
        events = [
            WorkflowStartedEvent(workflow_id="wf-1"),
            ZahirInternalErrorEvent(workflow_id="wf-1", error=None),  # No error message
        ]
        mock_queue.get.side_effect = events

        # Mock the processes to avoid actual multiprocessing
        with patch("zahir.worker.overseer.multiprocessing.Process") as mock_process:
            mock_proc = MagicMock()
            mock_proc.is_alive.return_value = False
            mock_process.return_value = mock_proc

            # Run the overseer
            with pytest.raises(RuntimeError, match="Unknown internal error in worker"):
                list(zahir_worker_overseer(job, context, worker_count=1))


def test_overseer_internal_error_with_error_message():
    """Test overseer handling ZahirInternalErrorEvent with error message (line 115)."""
    scope = LocalScope()
    scope.add_job_class(SimpleOutputJob)
    job_registry = SQLiteJobRegistry(":memory:")
    context = MemoryContext(scope=scope, job_registry=job_registry)

    job = SimpleOutputJob(input={}, dependencies={})

    with patch("zahir.worker.overseer.multiprocessing.Queue") as mock_queue_class:
        mock_queue = MagicMock()
        mock_queue_class.return_value = mock_queue

        # Create an error blob
        test_error = ValueError("Test error message")
        error_blob = exception_to_text_blob(test_error)

        events = [
            WorkflowStartedEvent(workflow_id="wf-1"),
            ZahirInternalErrorEvent(workflow_id="wf-1", error=error_blob),
        ]
        mock_queue.get.side_effect = events

        with patch("zahir.worker.overseer.multiprocessing.Process") as mock_process:
            mock_proc = MagicMock()
            mock_proc.is_alive.return_value = False
            mock_process.return_value = mock_proc

            # Run the overseer - should raise the deserialized error
            with pytest.raises(ValueError, match="Test error message"):
                list(zahir_worker_overseer(job, context, worker_count=1))


def test_overseer_keyboard_interrupt():
    """Test overseer handling KeyboardInterrupt (line 130-131)."""
    scope = LocalScope()
    scope.add_job_class(SimpleOutputJob)
    job_registry = SQLiteJobRegistry(":memory:")
    context = MemoryContext(scope=scope, job_registry=job_registry)

    job = SimpleOutputJob(input={}, dependencies={})

    with patch("zahir.worker.overseer.multiprocessing.Queue") as mock_queue_class:
        mock_queue = MagicMock()
        mock_queue_class.return_value = mock_queue

        # Make queue.get() raise KeyboardInterrupt
        mock_queue.get.side_effect = KeyboardInterrupt()

        with patch("zahir.worker.overseer.multiprocessing.Process") as mock_process:
            mock_proc = MagicMock()
            mock_proc.is_alive.return_value = False
            mock_process.return_value = mock_proc

            # Run the overseer - should handle KeyboardInterrupt gracefully
            result = list(zahir_worker_overseer(job, context, worker_count=1))

            # Should return empty list (no events yielded after interrupt)
            assert result == []

            # Shutdown should have been called
            mock_proc.is_alive.assert_called()


def test_overseer_all_events_flag():
    """Test overseer with all_events=True yields WorkflowCompleteEvent."""
    scope = LocalScope()
    scope.add_job_class(SimpleOutputJob)
    job_registry = SQLiteJobRegistry(":memory:")
    context = MemoryContext(scope=scope, job_registry=job_registry)

    job = SimpleOutputJob(input={}, dependencies={})

    with patch("zahir.worker.overseer.multiprocessing.Queue") as mock_queue_class:
        mock_queue = MagicMock()
        mock_queue_class.return_value = mock_queue

        events = [
            WorkflowStartedEvent(workflow_id="wf-1"),
            WorkflowCompleteEvent(workflow_id="wf-1", duration_seconds=5.0),
        ]
        mock_queue.get.side_effect = events

        with patch("zahir.worker.overseer.multiprocessing.Process") as mock_process:
            mock_proc = MagicMock()
            mock_proc.is_alive.return_value = False
            mock_process.return_value = mock_proc

            # Overseer now always emits all events
            result = list(zahir_worker_overseer(job, context, worker_count=1))

            # Should yield both WorkflowStartedEvent and WorkflowCompleteEvent
            assert len(result) == 2
            assert isinstance(result[0], WorkflowStartedEvent)
            assert isinstance(result[1], WorkflowCompleteEvent)


def test_overseer_yields_workflow_output_events():
    """Test that overseer yields WorkflowOutputEvent and ZahirCustomEvent."""
    scope = LocalScope()
    scope.add_job_class(SimpleOutputJob)
    job_registry = SQLiteJobRegistry(":memory:")
    context = MemoryContext(scope=scope, job_registry=job_registry)

    job = SimpleOutputJob(input={}, dependencies={})

    with patch("zahir.worker.overseer.multiprocessing.Queue") as mock_queue_class:
        mock_queue = MagicMock()
        mock_queue_class.return_value = mock_queue

        events = [
            WorkflowStartedEvent(workflow_id="wf-1"),
            WorkflowOutputEvent(output={"data": "value"}),
            ZahirCustomEvent(output={"custom": "event"}),
            WorkflowCompleteEvent(workflow_id="wf-1", duration_seconds=5.0),
        ]
        mock_queue.get.side_effect = events

        with patch("zahir.worker.overseer.multiprocessing.Process") as mock_process:
            mock_proc = MagicMock()
            mock_proc.is_alive.return_value = False
            mock_process.return_value = mock_proc

            # Overseer now always emits all events
            result = list(zahir_worker_overseer(job, context, worker_count=1))

            # Should yield all events including WorkflowStartedEvent, output events, and WorkflowCompleteEvent
            assert len(result) == 4
            assert isinstance(result[0], WorkflowStartedEvent)
            assert isinstance(result[1], WorkflowOutputEvent)
            assert isinstance(result[2], ZahirCustomEvent)
            assert isinstance(result[3], WorkflowCompleteEvent)
