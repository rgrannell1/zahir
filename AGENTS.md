
We use `../orbius` as an algebraic effects library
We use `../uqbar` (project name tertius) as a algebraic effects erlang runtime

**Communication**
- No sychophantic language please
- Use British English
- Do not remove my docstrings

**Coding style**

- Do not delete comments, it's annoying. I use them to make it obvious what a block of code is intended to do
- Never use single-letter variables
- I name exceptions `err` and indices `idx`, `jdx`, etc.
- Functions must be short and single purpose.
- No mid file imports
- Avoid deeply nested lines
- use the constants file for constants. document with a plain english line comment what the thing represents. group constants in a block.
- Avoid deeply nested lines
- Do not write large functions. Split into subfunctions
- Do not write inner functions; use partial application instead
- Avoid using optional, or `X | None = None` unless there's a direct need for it

**Testing**

- Use pytest
- Factor out test-data creation from test assertions
- Tests must have description strings like  "Proves <general system property>"
- ux tests should just be added by me, on request. Normally, create tests in tests/

**Tools & build**

- `rs` is my main build system
- Set up `uv`, `ruff`
- Always use `uv run python`, never `python` or `python3`
- Use `sqlite` CLI command, not `sqlite3`

---

**Design**

- We can only emit telemetry using EEmit( <inner_data> )
- We can only observe the system by looking at effects (zahir's or tertius's)
- What does not have an effect / event, cannot be observed
- Telemetry can only be added by decorating a handler. The handlers logic must be identical to before, including erroring, with the sole exception that more EEmit's are allowed

- We may yield zahir effect internally that have handlers yielding their own zahir / tertius effects
- Not all effects are sent by jobs themselves; most are not
