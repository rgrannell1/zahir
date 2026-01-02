from zahir.worker.state_machine.states import StartStateChange, StateChange


def handle_job_output(state) -> tuple[StateChange, None]:
    """We received a job output! It's emitted upstream already; just null out the job state. Persist the output to the state if awaited; we'll pop, then pass
    the output to the awaiting job"""

    # silly to store twice
    state.context.job_registry.set_output(
        state.frame.job.job_id, state.workflow_id, state.output_queue, state.last_event.output
    )

    # Jobs can only output once, so clear the job and generator
    state.frame = None

    return StartStateChange({"message": "Setting job output"}), state
