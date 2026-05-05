
# Evaluate

We ultimately have to run a workflow. Code in this folder does that.

**coordination_handlers.pt**

We yield effects to the overseer; these handlers wire effect responses into the overseer storage effects layer.

**job_handlers.py**

We decouple effects (the messaging layer) from handlers. Defines what job effects do, some output checks, and timeout handling.

**overseer.py**

Spawns a tertius genserver to coordinate messages back and forward between the storage backend and jobs across processes. Workers ask the overseer for jobs.

**runner.py**

The entrypoint to Zahir. Applies handlers, uses tertius and orbis.

**suspension.py**

`EAwait` pauses a job while we await its subjob result(s). `SuspensionTable` offers a `resume` and `suspend` method to do this.

**worker.py**

This is what we run per-process on top of a Tertius genserver. It's a simple state machine, with running and idle states.

_Idle_
Call `EGetJob`, receive:
- None: stay idle for a while
- WorkItemTag.JOB: construct a RunningJob, flip to running state
- WorkItemTag.RESULT: we received a result for a previously suspended job; look it up, pull it out as `Ok`/`Err`, and transition to Running.

_Running_
Advance the job one step, or throw.
- StopIteration: We're done, release concurrency slots, and broadbast `EJobComplete`. Flip to Idle.
- Other exception: Broadcast `EJobFailed`, flip to Idle
- `_Suspended`: The job is parked in the suspension table as we're awaiting, flip to Idle.
