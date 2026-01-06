import time

from zahir.worker.state_machine.states import StartStateChange
import random


def wait_for_job(state) -> tuple[StartStateChange, None]:
    """No jobs available; sleep briefly before checking again."""

    # Add jitter to reduce simultaneous claims on jobs
    jitter = random.random() * 0.1
    time.sleep(0.25 + jitter)

    return StartStateChange({"message": "Waited for job, restarting"}), state
