
# Róisín's Agents

- Do not say I'm absolutely right, totally correct, etc.
- Never use single-letter variables
- No private functions. Functions should not start with _; do not make them private.
- I name exceptions `err` and indices `idx`, `jdx`, etc.
- Functions must be short and single purpose.
- Use British English

## Python

- Set up `uv`, `ruff`
- I only use `plotnine` for visualisations
- In Jupyter, define functions. No top level code where possible.

## Builds

- On typescript projects, prefer the esbuild cli in a `bs` folder
- `rs` is my main build system
- To add a build script, put an executable zsh file with a shebang in bs/<name>.zsh
- I like having `run`, `dev:watch`, `build`, `lint`, `check`, `test` commands defined
- If I ask you to create a `bs` build system, create a `run.sh` and `test.sh` file if appropriate

Instructions for Zahir Specifically

## General

- Do not write documentation
- Do not write comments or comment strings
- /home/rg/Code/mirror/commands is the most advanced example of usage, and I'll often refer to it during debugging. The database is `/home/rg/Code/mirror/mirror_jobs.db`
- `Sqlite` is the CLI command, not `sqlite3`

## Testing

- Run tests with `rs test`. If broken, it may not terminate, so handle this with `timeout`.
- ALWAYS use `uv run python3` to run python files
- Lint with `rs lint`
- Type check with `rs check && rs check:mypy`
- You can clear tempfiles if you need to
- Use `ZAHIR_LOG_LEVEL=DEBUG` to see more logs
- Each test should have a short, clear explanation of what it proves as a documentation string
- Do not simply delete failing tests; explain why you are removing at the very least

