
# Effects

Effects are events with responses, approximately. Layering of job effects from coordination effects insulates jobs from directly altering Zahir runtime internals in-band (the better approach would be modifying handlers)

**coordination.py**

Effects used by the workers to communicate with the overseer process. Not yielded by jobs directly, though some are thin translations of job effects.

- `EAcquireSlot(name, limit)`: request a named concurrency slot from the overseer
- `ESignal(name)`: query the current state of a named semaphore from the overseer
- `ESetSemaphoreState(name, state)`: write a new state for a named semaphore to the overseer
- `EIsDone()`: ask the overseer whether all pending jobs have completed
- `EGetError()`: retrieve the root error from the overseer, if any
- `EGetResult()`: retrieve the root job's return value from the overseer

**job.py**

Effects a job may yield.

- `EGetJob(worker_pid_bytes)`: request work from the overseer — returns a new job, a buffered child result, or None
- `EEnqueue(fn_name, args, reply_to, timeout_ms, sequence_number)`: queue a job; reply_to and sequence_number are None for the root job, set for child jobs
- `EJobComplete(result, reply_to, sequence_number)`: report successful job completion to the overseer
- `EJobFail(error, reply_to, sequence_number)`: report job failure (error or timeout) to the overseer
- `ERelease(name)`: release a named concurrency slot back to the overseer

**storage.py**

Storage effects are yielded to the backend to handle stateful storage.

The overseer talks to the job queue through these effects rather than calling it directly. This means we can swap out storage implementations by swapping the handlers for these effects.

- `EStorageGetJob(worker_pid_bytes)`: hand the next pending job to a worker that is ready for one
- `EStorageEnqueue(fn_name, args, reply_to, timeout_ms, sequence_number)`: add a child job to the queue and note that one more result is expected
- `EStorageJobDone(reply_to, sequence_number, body)`: mark one job finished, send its result back to whoever is waiting, or keep it as the final output if it was the root job
- `EStorageJobFailed(error)`: mark one job finished with a failure and record it as the root-level error
- `EStorageAcquire(name, limit)`: claim one of the available slots for a named concurrency limit, blocking further work if none are free
- `EStorageRelease(name)`: give back a slot for a named concurrency limit
- `EStorageSignal(name)`: check how many slots are currently in use for a named concurrency limit
- `EStorageSetSemaphore(name, state)`: directly set the slot state for a named concurrency limit
- `EStorageIsDone()`: ask whether every job has finished
- `EStorageGetError()`: fetch the recorded failure, if the run went wrong
- `EStorageGetResult()`: fetch the return value of the root job once it has finished
