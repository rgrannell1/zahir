
All telemetry comes from the effects system. System statistics (CPU, RAM, active cores) are the only exception.

Jobs fan out work in parallel using EAwait and receive results back in dispatch order regardless of which job finishes first. A crashing child job surfaces as a JobError to any job awaiting its result.

Jobs communicate results to the caller by yielding EEmit. The evaluate function surfaces these events to the caller as a stream.

Job functions must be defined at module level so that worker processes can load and run them. A locally-scoped function fails with a clear error rather than silently hanging. Passing a function name not present in scope raises KeyError immediately at evaluate time.

A job that yields an invalid effect — including raw tertius effects like EReceive — raises InvalidEffect rather than blocking or hanging. A job that catches InvalidEffect can continue executing.

Jobs run across multiple OS processes so that work distributes across CPU cores.

The root job's return value is surfaced by evaluate as the final item in the event stream, after all emitted events. If the root job returns None, nothing extra is yielded.

Tertius mcall and mcast are never used outside of effect handler functions; this would show an effect is missing

Each process may call handle exactly once. 

The progress bar shows mean execution time for each job type

All statistics must be computed through bookman.

Root-job seeding uses EEnqueue rather than a dedicated EStorageInitialize effect, so the root job enters the queue the same way child jobs do. EEnqueue allows its reply_to and sequence_number fields to be absent for the root-job case. No mcast call appears outside a handler function.
