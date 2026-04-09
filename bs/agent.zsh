#!/usr/bin/env zsh
set -euo pipefail

cp  ~/Agents/AGENTS.md ./AGENTS.md
cat <<"EOF" >> ./AGENTS.md

Instructions for Zahir

## General

- Do not write documentation
- Do not write comments or comment strings
- Use `sqlite` CLI command to run queries, not `sqlite3`

## Usage

- /home/rg/Code/mirror/commands is the most advanced Zahir workflow, and I'll often refer to it during debugging. The database is `/home/rg/Code/mirror/mirror_jobs.db`

## Testing

- Run tests with `rs test`. If broken, it may not terminate, so handle this with `timeout`.
- ALWAYS use `uv run python3` to run python files
- Lint with `rs lint`
- Type check with `rs check && rs check:mypy`
- Clear specific tempfiles if needed
- Use `ZAHIR_LOG_LEVEL=DEBUG` to see more logs
- Each test must have a short, clear comment string explaining what it proves

EOF
