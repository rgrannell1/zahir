#!/usr/bin/env python3
"""Test if semaphore works correctly across multiple processes."""

import multiprocessing
import time
from typing import Any

from zahir.base_types import DependencyState
from zahir.dependencies.semaphore import Semaphore


def process_worker(process_id: int, semaphore_id: str, shared_state: dict[str, Any], results: dict[str, Any]) -> None:
    """Worker function that checks semaphore state."""

    # Recreate the shared context in this process
    class SharedContext:
        def __init__(self, state_dict):
            self.state = state_dict

    shared_context = SharedContext(shared_state)

    # Recreate the semaphore with the shared context
    semaphore = Semaphore(context=shared_context, initial_state=DependencyState.SATISFIED)
    semaphore.semaphore_id = semaphore_id

    # Simulate some work
    time.sleep(0.1 * process_id)

    # Check current state
    current_state = semaphore.satisfied()
    results[f"process_{process_id}_initial"] = current_state.state.value

    # Try to modify state
    if process_id == 0:
        semaphore.close()
        results[f"process_{process_id}_action"] = "closed"
    elif process_id == 1:
        semaphore.abort()
        results[f"process_{process_id}_action"] = "aborted"
    elif process_id == 2:
        semaphore.open()
        results[f"process_{process_id}_action"] = "opened"

    # Check state again
    time.sleep(0.1)
    final_state = semaphore.satisfied()
    results[f"process_{process_id}_final"] = final_state.state.value


def test_semaphore_multiprocess_with_context():
    """Test semaphore with context.state (should succeed - new behavior)."""

    print("=" * 60)
    print("Testing semaphore across 5 processes with context.state")
    print("=" * 60)

    # Create a shared context state using multiprocessing Manager
    manager = multiprocessing.Manager()
    shared_state = manager.dict()
    results = manager.dict()

    # Create a minimal context-like object with state
    class SharedContext:
        def __init__(self, state_dict):
            self.state = state_dict

    shared_context = SharedContext(shared_state)

    # Create a semaphore with shared context
    semaphore = Semaphore(context=shared_context, initial_state=DependencyState.SATISFIED)
    semaphore_id = semaphore.semaphore_id

    # Initialize the state in shared context
    state_key = semaphore._get_state_key()
    shared_state[state_key] = DependencyState.SATISFIED.value

    # Create and start 5 processes
    processes = []
    for idx in range(5):
        process = multiprocessing.Process(target=process_worker, args=(idx, semaphore_id, shared_state, results))
        processes.append(process)
        process.start()

    # Wait for all processes to complete
    for process in processes:
        process.join()

    # Print results
    print("\nResults:")
    for key in sorted(results.keys()):
        print(f"  {key}: {results[key]}")

    # Check final state in shared context
    state_key = f"semaphore_{semaphore_id}"
    final_shared_state = shared_state.get(state_key, "unknown")
    print(f"\nFinal state in context.state: {final_shared_state}")

    # Analyze: Did modifications from different processes persist?
    # Process 0 closes (unsatisfied), but then process 1 aborts (impossible),
    # then process 2 opens (satisfied). So final should be satisfied.
    process_0_action = results.get("process_0_action")
    process_1_action = results.get("process_1_action")
    process_2_action = results.get("process_2_action")

    success = (
        process_0_action == "closed"
        and process_1_action == "aborted"
        and process_2_action == "opened"
        and final_shared_state == "satisfied"
    )

    if success:
        print("\n✓ Semaphore modifications ARE shared across processes!")
        print("  Process 0: closed (unsatisfied)")
        print("  Process 1: aborted (impossible)")
        print("  Process 2: opened (satisfied)")
        print(f"  Final state: {final_shared_state}")
    else:
        print("\n✗ Semaphore modifications not properly shared")
        print(f"  Final state should be 'satisfied' but got: {final_shared_state}")

    return success


if __name__ == "__main__":
    success = test_semaphore_multiprocess_with_context()
    exit(0 if success else 1)
