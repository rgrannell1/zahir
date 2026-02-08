"""Zahir type definitions, split into focused modules.

This package contains the core type hierarchy for Zahir:

- dependency: Dependency ABC, DependencyState, DependencyData
- job_state: JobState enum, state sets, coverage validation
- registry: JobRegistry ABC, EventRegistry ABC, JobInformation, JobTimingInformation
- scope: Scope ABC
- transform: Transform type alias, TransformSpec dataclass
- serialised: SerialisedTransformSpec, SerialisedJobData, SerialisedJobInstance
- context: Context dataclass
- job: JobSpec, JobInstance, JobArguments, type aliases, validation
"""

from zahir.types.context import Context
from zahir.types.dependency import Dependency, DependencyData, DependencyState
from zahir.types.job import (
    JobArguments,
    JobEventSet,
    JobInstance,
    JobSpec,
    Precheck,
    Recover,
    Run,
    create_typeddict_precheck,
    default_recover,
    validate_output_type,
)
from zahir.types.job_state import ACTIVE_JOB_STATES, COMPLETED_JOB_STATES, JobState, check_job_states_coverage
from zahir.types.registry import EventRegistry, JobInformation, JobRegistry, JobTimingInformation
from zahir.types.scope import Scope
from zahir.types.serialised import SerialisedJobData, SerialisedJobInstance, SerialisedTransformSpec
from zahir.types.transform import Transform, TransformSpec

__all__ = [
    "ACTIVE_JOB_STATES",
    "COMPLETED_JOB_STATES",
    "Context",
    "Dependency",
    "DependencyData",
    "DependencyState",
    "EventRegistry",
    "JobArguments",
    "JobEventSet",
    "JobInformation",
    "JobInstance",
    "JobRegistry",
    "JobSpec",
    "JobState",
    "JobTimingInformation",
    "Precheck",
    "Recover",
    "Run",
    "Scope",
    "SerialisedJobData",
    "SerialisedJobInstance",
    "SerialisedTransformSpec",
    "Transform",
    "TransformSpec",
    "check_job_states_coverage",
    "create_typeddict_precheck",
    "default_recover",
    "validate_output_type",
]
