"""SQLite-based registry for persistent workflow execution."""

import json
import sqlite3
from pathlib import Path
from threading import Lock
from typing import Iterator

from zahir import events as event_module
from zahir.events import ZahirEvent, WorkflowOutputEvent
from zahir.types import (
    Context,
    DependencyState,
    EventRegistry,
    Job,
    JobRegistry,
    JobState,
)
