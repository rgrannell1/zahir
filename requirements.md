


All telemetry comes from the effects system. System statistics (CPU, RAM, active cores) are the only exception.

The public API exposes no zahir-defined classes other than errors, effects, and JobContext. Callers never subclass JobContext; it exists only as a type annotation for the context object passed to job functions.

Jobs fan out work in parallel using EAwaitAll and receive results back in dispatch order regardless of which job finishes first. A crashing child job surfaces as a JobError to any job awaiting its result.

Jobs communicate results to the caller by yielding EEmit. The evaluate function surfaces these events to the caller as a stream.

Job functions must be defined at module level so that worker processes can load and run them. A locally-scoped function fails with a clear error rather than silently hanging. Passing a function name not present in scope raises KeyError immediately at evaluate time.

A job that yields an invalid effect — including raw tertius effects like EReceive — raises InvalidEffect rather than blocking or hanging. A job that catches InvalidEffect can continue executing.

Jobs run across multiple OS processes so that work distributes across CPU cores. Fanning out enough parallel jobs results in execution on more than one OS process.

The await API exposes a single construct, EAwait. A user may write `yield ctx.scope.myjob()`, `yield EAwait(ctx.scope.myjob())`, or `yield EAwait([ctx.scope.myjob(), ...])` and all three forms are equivalent. EAwait accepts a bare EAwait instance or a list of EAwait instances and normalises them internally. EAwaitAll is not part of the user-facing API.

The root job's return value is surfaced by evaluate as the final item in the event stream, after all emitted events. If the root job returns None, nothing extra is yielded.

The tertius primitives mcall and mcast are never used directly outside of effect handler functions. All communication with the overseer goes through the effects system; a raw mcall or mcast is a sign that a coordination effect is missing.

The progress bar displays the mean execution time for each job type, shown as μXs ahead of the function name. The column is fixed-width so function names remain aligned across all rows. The mean is derived from completed job spans only, not coordination effects.

The per-function job counts (total enqueued, completed, failed) tracked by the progress bar are defined as bookman aggregators in the metrics module, composed with zip_all, filter_events, and count_distinct. The progress bar state model applies these aggregators incrementally as events arrive rather than maintaining bespoke imperative counters.

The ProgressBarService derives both per-function job counts and mean durations from a single per_fn_progress_agg() aggregator, consuming the job_lifecycle span events already emitted by the telemetry wrapper. TimeEstimator is removed; the ETA computation it contained lives in ProgressBarService, deriving in-flight counts from the aggregator state rather than tracking them separately. ProgressBarState uses per_fn_progress_agg() in place of job_stats_agg(), making mean_ms available from JobStats directly. Tests previously covering TimeEstimator are rewritten against the new interface.

The job evaluation pipeline is built from three composable generator layers rather than a single manual dispatch loop. An invalid effect guard wraps the job generator directly, throwing InvalidEffect for any effect the job should not yield — non-Effect values, coordination effects like EEnqueue, or blocked tertius effects like EReceive. Because this guard wraps the job generator before the handler layer, effects emitted by the job handlers themselves (such as EAcquireSlot from the acquire handler) bypass it. A handle() call sits above the guard and dispatches job-level effects like EAcquire, EGetSemaphore, and ESetSemaphore to their handlers. A timeout guard sits outermost, checking the job deadline on each effect that reaches it and injecting JobTimeout if the deadline has passed. EAwait and unhandled coordination effects bubble through all three layers and are handled at the worker level as before.
