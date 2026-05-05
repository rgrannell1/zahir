
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

evaluate takes fn_name, args, and scope as positional arguments, and n_workers, user_context, handler_wrappers, and handlers as keyword-only arguments. There is no separate storage_handlers parameter; evaluate merges the default in-memory storage handlers with any user-supplied handlers and passes the result to all processes.

Error telemetry includes the full stack trace, not just the exception message. When a handler raises an exception, or when a job fails and its error is recorded on an EJobFail effect, the emitted telemetry event carries the complete formatted traceback so that errors are debuggable from telemetry alone.

Dependency condition functions return an explicit three-state result — satisfied, unsatisfied, or impossible — each carrying optional dict metadata. All three states support metadata so that callers can attach contextual information regardless of outcome. The dependency combinator continues polling while a condition returns unsatisfied; the check combinator treats unsatisfied as impossible. The DependencyResult type seen by callers outside the dependency system remains a two-state satisfied-or-impossible value.
