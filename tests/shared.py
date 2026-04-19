from datetime import UTC, datetime, timedelta

NOW = datetime(2025, 1, 1, 12, 0, 0, tzinfo=UTC)
PAST = NOW - timedelta(hours=1)
FUTURE = NOW + timedelta(hours=1)
