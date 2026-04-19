
Read `/home/rg/zahir/README.md` for context

**Communication**
- No sychophantic language please
- Use British English
- Do not remove my docstrings

**Coding style**

- Never use single-letter variables
- I name exceptions `err` and indices `idx`, `jdx`, etc.
- Functions must be short and single purpose.
- No mid file imports
- Avoid deeply nested lines
- use the constants file for constants. document with a plain english line comment what the thing represents. group constants in a block.
- Avoid deeply nested lines
- Do not write large functions. Split into subfunctions
- Do not write inner functions; use partial application instead

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
