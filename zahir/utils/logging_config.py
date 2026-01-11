"""Centralized logging configuration for Zahir."""

import logging
import os
import sys


def configure_logging() -> None:
    """Configure logging for all Zahir processes.

    Respects ZAHIR_LOG_LEVEL environment variable:
    - DEBUG: Verbose logging
    - INFO: Info and above
    - WARNING: Warning and above (default)
    - ERROR: Error and above
    """
    log_level_name = os.getenv("ZAHIR_LOG_LEVEL", "WARNING").upper()
    log_level = getattr(logging, log_level_name, logging.WARNING)

    logging.basicConfig(
        level=log_level,
        stream=sys.stderr,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    # Set level on all existing handlers
    for handler in logging.getLogger().handlers:
        handler.setLevel(log_level)


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)
