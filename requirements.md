


All telemetry comes from the effects system. System statistics (CPU, RAM, active cores) are the only exception.

Jobs fan out work in parallel using EAwaitAll and receive results back in dispatch order regardless of which job finishes first. A crashing child job surfaces as a JobError to any job awaiting its result.

Jobs communicate results to the caller by yielding EEmit. The evaluate function surfaces these events to the caller as a stream.

Job functions must be defined at module level so that worker processes can load and run them. A locally-scoped function fails with a clear error rather than silently hanging. Passing a function name not present in scope raises KeyError immediately at evaluate time.

A job that yields an invalid effect — including raw tertius effects like EReceive — raises InvalidEffect rather than blocking or hanging. A job that catches InvalidEffect can continue executing.

Jobs run across multiple OS processes so that work distributes across CPU cores. Fanning out enough parallel jobs results in execution on more than one OS process.
