import time

from zahir.worker.state_machine.states import StartStateChange, StateChange


def wait_for_job(state) -> tuple[StateChange, None]:
    """No jobs available; for the moment let's just sleep. In future, be cleverer
    and have dependencies suggest nap-times"""

    time.sleep(1)

    return StartStateChange({"message": "Waited for job, restarting"}), state
