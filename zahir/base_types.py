"""Core type definitions used throughout Zahir.

This module re-exports all types from the zahir.types package for
backwards compatibility. New code should import from zahir.types
directly.
"""

# Re-export everything from the new split modules
from zahir.types.context import Context
from zahir.types.dependency import Dependency, DependencyData, DependencyResult, DependencyState
from zahir.types.job import (
    ArgsType,
    JobArguments,
    JobEventSet,
    JobInstance,
    JobSpec,
    OutputType,
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
    "ArgsType",
    "Context",
    "Dependency",
    "DependencyData",
    "DependencyResult",
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
    "OutputType",
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
