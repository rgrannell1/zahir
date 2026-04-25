# Zahir2 Architecture Review

## What's working well

**The three-tier effect hierarchy is coherent.** User effects → coordination effects → storage effects is a clean separation of concerns. Each layer genuinely doesn't need to know about the layers above it. The storage effects form a complete, testable contract independently of the process topology, which is why `MemoryBackend` can be tested in isolation.

**The overseer/suspension split is the right call.** The overseer owns globally-shared mutable state (queue, concurrency slots, semaphores, pending counts). Workers own local fan-out structure (which parent job is waiting for which child results). This avoids the overseer needing to model per-worker fan-out state, which would serialise fine-grained result routing and create contention. The indirect path (results buffered in `pending_results`, fetched on next `EGetJob`) is the price you pay and it's reasonable.

**The combinator/wrapper layer (`wrap`, `make_telemetry`) is genuinely composable.** Telemetry being entirely opt-in via `handler_wrappers` with zero overhead when unused is good discipline. The `reduce(lambda h, w: w(h), wrappers, h)` pattern is consistent everywhere.

**The dependency subsystem is well-bounded.** Each dependency is a self-contained generator — no implicit global state, no ambient context. `group_dependency` composing them naturally follows.

---

## Genuine architectural concerns

**`JobContext` is underspecified.** It's an empty class that the runtime populates at startup (`._scope`, `.scope`, `.handler_wrappers`). Users subclass it (mirror does this) but there's no contract for what fields are user-owned vs. runtime-injected. If a user subclass happens to use `scope` or `handler_wrappers` as field names, silent clobbering occurs. This should be a dataclass or typed class with explicit slots, with the runtime fields documented or separated.

**`EAwait` is conspicuously different from every other effect.** It's the only non-dataclass effect. Its `__init__` has three code paths (`_fields_from_passthrough`, `_fields_from_list`, direct `jobs=` form). This complexity isn't encapsulated — it's exposed to anyone reading `effects.py`. The three forms exist to support user ergonomics (`ctx.scope.fn()`, `[ctx.scope.fn(), ctx.scope.fn()]`) but that conversion could happen in `ScopeProxy` and the dependency layer, keeping `EAwait` a plain dataclass. Right now `EAwait.__init__` is doing dispatch that arguably belongs elsewhere.

**The `evaluate()` signature has a conceptual muddle.** `handler_wrappers` and `handlers` are two different extension mechanisms for different things — wrappers compose around handlers, raw `handlers` replace them entirely. A user passing both has to understand how they interact (raw handlers go in last, take precedence over coordination handlers, then wrappers are applied on top). These are different enough in semantics that having them next to each other in the same function signature without documentation invites mistakes.

**The public API surface is invisible.** All four `__init__.py` files are empty. Users have to know to import from `zahir.core.evaluate`, `zahir.core.dependencies.sqlite`, etc. There's no single `zahir` or `zahir.core` module that exports the stable public surface. This is a maintenance problem as well as a usability one — nothing distinguishes public API from internal implementation.

**Polling dependencies can't be woken.** When a job is sleeping waiting for a semaphore, the only path to resumption is the polling interval expiring. If something outside the worker sets a semaphore to `satisfied`, the waiting job won't notice for up to `DEPENDENCY_DELAY_MS` milliseconds. For semaphore dependencies specifically this is directly soluble — the `ESetSemaphore` handler already has a clear write path and could notify waiting workers. The current model is simple but has a latency floor baked in.

**Fan-out errors are lossy.** When multiple children in an `EAwait([...])` fail, only the first error surfaces. The rest are discarded. This is visible in `suspension.py` — results are collected, then `raise` on the first error. For workflows that want to understand the full failure distribution (e.g. N out of M jobs failed) this is a gap. It also means the root error (stored in `MemoryBackend.root_error`) is always only one failure, even when many occurred.

---

## Smaller observations

- `ScopeProxy` is clean but relies on `__getattr__` magic that makes the dispatch path non-obvious in debugging. It's the right ergonomic choice for the user-facing API, but it means `ctx.scope.nonexistent_fn(x)` will silently form an `EAwait` until the worker tries to run it and gets a `KeyError`.

- `_poll_completion` in `evaluate/__init__.py` is the root process's main loop and it polls on `COMPLETION_POLL_MS`. This means the overall workflow has a minimum observable latency after the last job completes before `evaluate()` returns — worth documenting.

- The `THROWABLE` constant (a union of exception types) used in `evaluate_job` is doing real work as the set of exceptions that get re-thrown into the job vs. propagated upward. It's important enough to warrant more than a constant name — the distinction matters for extension.

---

## Summary

The core model — generators as jobs, algebraic effects for coordination, an Erlang-style overseer, pluggable storage — is well-conceived and internally consistent. The main structural debt is around the edges: a weak public API boundary, an underspecified context contract, and `EAwait`'s complexity bleeding out of where it should live. None of these are acute, but the public API gap is probably the one most worth addressing before this gets used more widely by callers outside mirror.





> **`JobContext` is underspecified.** It's an empty class that the runtime populates at startup (`._scope`, `.scope`, `.handler_wrappers`). Users subclass it (mirror does this) but there's no contract for what fields are user-owned vs. runtime-injected. If a user subclass happens to use `scope` or `handler_wrappers` as field names, silent clobbering occurs. This should be a dataclass or typed class with explicit slots, with the runtime fields documented or separated.

fair, recommendations? i want to avoid people subclassing things

> - `ScopeProxy` is clean but relies on `__getattr__` magic that makes the dispatch path non-obvious in debugging. It's the right ergonomic choice for the user-facing API, but it means `ctx.scope.nonexistent_fn(x)` will silently form an `EAwait` until the worker tries to run it and gets a `KeyError`.

bug, can we ensure the thing exists when getattred? write a failing test, fix

> **Fan-out errors are lossy.** When multiple children in an `EAwait([...])` fail, only the first error surfaces. The rest are discarded. This is visible in `suspension.py` — results are collected, then `raise` on the first error. For workflows that want to understand the full failure distribution (e.g. N out of M jobs failed) this is a gap. It also means the root error (stored in `MemoryBackend.root_error`) is always only one failure, even when many occurred.

not ideal, but JS promise does this too.


> **The public API surface is invisible.** All four `__init__.py` files are empty. Users have to know to import from `zahir.core.evaluate`, `zahir.core.dependencies.sqlite`, etc. There's no single `zahir` or `zahir.core` module that exports the stable public surface. This is a maintenance problem as well as a usability one — nothing distinguishes public API from internal implementation.


fix




---

> **`EAwait` is conspicuously different from every other effect.** It's the only non-dataclass effect. Its `__init__` has three code paths (`_fields_from_passthrough`, `_fields_from_list`, direct `jobs=` form). This complexity isn't encapsulated — it's exposed to anyone reading `effects.py`. The three forms exist to support user ergonomics (`ctx.scope.fn()`, `[ctx.scope.fn(), ctx.scope.fn()]`) but that conversion could happen in `ScopeProxy` and the dependency layer, keeping `EAwait` a plain dataclass. Right now `EAwait.__init__` is doing dispatch that arguably belongs elsewhere.

VERY TRUE

---



> **The `evaluate()` signature has a conceptual muddle.** `handler_wrappers` and `handlers` are two different extension mechanisms for different things — wrappers compose around handlers, raw `handlers` replace them entirely. A user passing both has to understand how they interact (raw handlers go in last, take precedence over coordination handlers, then wrappers are applied on top). These are different enough in semantics that having them next to each other in the same function signature without documentation invites mistakes.
