from dataclasses import dataclass

from zahir.base_types import (
    JobRegistry,
)


@dataclass
class ZahirLogger:
    job_registry: "JobRegistry"
