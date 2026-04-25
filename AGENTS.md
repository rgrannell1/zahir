@/home/rg/Agents/AGENTS.md
@/home/rg/Agents/agents.python.md

We use `../orbis` as an algebraic effects library.
We use `../tertius` (project name tertius) as an algebraic effects erlang runtime.
We use `../sand` (project name bookman) to structure our telemetry.
`../mirror` is our main dependent; we'll often debug it.

## Effects & Telemetry

- We can only emit telemetry using `EEmit( <inner_data> )`
- We can only observe the system by looking at effects (zahir's or tertius's)
- What does not have an effect / event, cannot be observed
- Telemetry can only be added by decorating a handler. The handler's logic must be identical to before, including erroring, with the sole exception that more `EEmit`s are allowed
- We may yield zahir effects internally that have handlers yielding their own zahir / tertius effects
- Not all effects are sent by jobs themselves; most are not. We have subclasses denoting this behaviour
