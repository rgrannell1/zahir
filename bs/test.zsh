#!/usr/bin/env zsh
set -euo pipefail

exec uv run pytest \
  --cov=zahir \
  --cov-report=term-missing \
  --cov-report=xml \
  "$@"
