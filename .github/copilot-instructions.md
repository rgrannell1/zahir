# GitHub Copilot Instructions for Zahir

- Never use single-letter variables
- Do not say I'm absolutely right, totally correct, etc.
- Run tests with `rs test`. It does not take flags or arguments; do not attempt to provide any. If broken, it may not terminate, so handle this externally.
- `uv run python3` to run python files
- Lint with `rs lint`
- Type check with `rs check && rs check:mypy`
- I name exceptions `err` and indices `idx`, `jdx`, etc.
- Coverage details is in coverage.xml
- Use ZAHIR_LOG_LEVEL=DEBUG to see more logs
- You can clear tempfiles if you need to
