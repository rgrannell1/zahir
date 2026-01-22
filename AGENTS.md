# Instructions for Zahir


## General

- Read README.md for context
- Do not say I'm absolutely right, totally correct, etc.
- Never use single-letter variables
- Functions should not start with _; do not make them private
- I name exceptions `err` and indices `idx`, `jdx`, etc.
- Functions should be short and single purpose

## Testing

- Run tests with `rs test`. If broken, it may not terminate, so handle this with `timeout`.
- ALWAYS use `uv run python3` to run python files
- Lint with `rs lint`
- NEVER use `tail`, never. STOP USING TAIL!! NEVER USE HEAD!!!!!
- Type check with `rs check && rs check:mypy`
- If I paste in a fragment of a broken test, try to print out a `uv run pytest ` command so I can recheck it quickly myself
- Try to write tests that actually prove things
- Let me know if you significantly change a tests purpose
- Do not simply delete failing tests; explain why you are removing at the very least
- Use `ZAHIR_LOG_LEVEL=DEBUG` to see more logs
- You can clear tempfiles if you need to
