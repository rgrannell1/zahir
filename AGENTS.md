# Instructions for Zahir

Read ~/Agents/AGENTS.md

## General

- Read README.md for context
- Be aware of /home/rg/Code/rs's readme
- When debugging or investigating, track thoughts and observations in a markdown file
- Extend opentelemetry where helpful
- /home/rg/Code/mirror/commands is the most advanced example of usage, and I'll often refer to it during debugging. The database is `/home/rg/Code/mirror/mirror_jobs.db`
- Sqlite is the CLI command, not sqlite3

## Testing

- Run tests with `rs test`. If broken, it may not terminate, so handle this with `timeout`.
- ALWAYS use `uv run python3` to run python files
- Lint with `rs lint`
- Type check with `rs check && rs check:mypy`
- If I paste in a fragment of a broken test, try to print out a `uv run pytest ` command so I can recheck it quickly myself
- Try to write tests that actually prove things
- Let me know if you significantly change a tests purpose
- Do not simply delete failing tests; explain why you are removing at the very least
- Use `ZAHIR_LOG_LEVEL=DEBUG` to see more logs
- You can clear tempfiles if you need to
- We have opentelemetry traces and log files. clear these as needed.

## Planning

- We do not plan to add multi-machine support.
- I like time-series and histograms; i dislike counters. We have multiple processes; it's good not to just average their behaviour
