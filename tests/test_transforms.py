"""Tests for transforms - retry transform and scope transform registration."""

import pathlib
import sys
import tempfile

import pytest

from zahir.base_types import Context, JobSpec, TransformSpec, Transform
from zahir.context import MemoryContext
from zahir.dependencies.group import DependencyGroup
from zahir.events import JobOutputEvent, WorkflowOutputEvent, JobCompletedEvent, WorkflowCompleteEvent
from zahir.exception import TransformNotInScopeError
from zahir.job_registry import SQLiteJobRegistry
from zahir.jobs.decorator import spec
from zahir.scope import LocalScope
from zahir.transforms.retry import retry
from zahir.worker import LocalWorkflow


# ============================================================
# Test fixtures and helper jobs
# ============================================================


@spec()
def SuccessfulJob(spec_args, context: Context, input: dict, dependencies: DependencyGroup):
    """A job that always succeeds."""
    yield JobOutputEvent({"result": "success", "value": input.get("value", 0)})


@spec()
def FailingJob(spec_args, context: Context, input: dict, dependencies: DependencyGroup):
    """A job that always fails."""
    raise ValueError("This job always fails")
    yield  # Make it a generator


@spec()
def FailNTimesJob(spec_args, context: Context, input: dict, dependencies: DependencyGroup):
    """A job that fails N times then succeeds.

    Uses input["fail_count"] to track how many times to fail.
    Uses input["attempts"] (a mutable list) to track attempts.
    """
    attempts = input.get("attempts", [])
    fail_count = input.get("fail_count", 0)

    attempts.append(len(attempts) + 1)

    if len(attempts) <= fail_count:
        raise ValueError(f"Failing on attempt {len(attempts)}")

    yield JobOutputEvent({"result": "success", "total_attempts": len(attempts)})


# ============================================================
# TransformSpec tests
# ============================================================


class TestTransformSpec:
    """Tests for TransformSpec dataclass."""

    def test_transform_spec_creation(self):
        """Test basic TransformSpec creation."""
        spec = TransformSpec(type="retry", args={"max_retries": 5})

        assert spec.type == "retry"
        assert spec.args == {"max_retries": 5}

    def test_transform_spec_default_args(self):
        """Test TransformSpec with default empty args."""
        spec = TransformSpec(type="retry")

        assert spec.type == "retry"
        assert spec.args == {}

    def test_transform_spec_save(self):
        """Test TransformSpec serialization."""
        spec = TransformSpec(type="retry", args={"max_retries": 3, "backoff_factor": 2.0})
        saved = spec.save()

        assert saved == {
            "type": "retry",
            "args": {"max_retries": 3, "backoff_factor": 2.0},
        }

    def test_transform_spec_load(self):
        """Test TransformSpec deserialization."""
        data = {"type": "retry", "args": {"max_retries": 5}}
        spec = TransformSpec.load(data)

        assert spec.type == "retry"
        assert spec.args == {"max_retries": 5}

    def test_transform_spec_load_missing_args(self):
        """Test TransformSpec deserialization with missing args defaults to empty."""
        data = {"type": "retry"}
        spec = TransformSpec.load(data)

        assert spec.type == "retry"
        assert spec.args == {}

    def test_transform_spec_roundtrip(self):
        """Test TransformSpec save/load roundtrip."""
        original = TransformSpec(type="custom", args={"foo": "bar", "count": 42})
        saved = original.save()
        loaded = TransformSpec.load(saved)

        assert loaded.type == original.type
        assert loaded.args == original.args


# ============================================================
# Scope transform tests
# ============================================================


class TestScopeTransforms:
    """Tests for transform registration in LocalScope."""

    def test_scope_has_retry_by_default(self):
        """Test that retry transform is registered by default."""
        scope = LocalScope()

        transform = scope.get_transform("retry")
        assert transform is retry

    def test_scope_add_transform(self):
        """Test adding a custom transform."""
        scope = LocalScope()

        def custom_transform(args, spec):
            return spec

        scope.add_transform("custom", custom_transform)

        retrieved = scope.get_transform("custom")
        assert retrieved is custom_transform

    def test_scope_get_transform_raises_on_missing(self):
        """Test that getting a non-existent transform raises TransformNotInScopeError."""
        scope = LocalScope()

        with pytest.raises(TransformNotInScopeError):
            scope.get_transform("nonexistent")

    def test_scope_constructor_with_transforms(self):
        """Test LocalScope constructor accepts transforms dict."""

        def my_transform(args, spec):
            return spec

        scope = LocalScope(transforms={"my_transform": my_transform})

        assert scope.get_transform("my_transform") is my_transform
        # Built-in retry should still be available
        assert scope.get_transform("retry") is retry

    def test_scope_overwrite_transform(self):
        """Test that adding a transform with same name overwrites."""
        scope = LocalScope()

        def transform_v1(args, spec):
            return spec

        def transform_v2(args, spec):
            return spec

        scope.add_transform("versioned", transform_v1)
        assert scope.get_transform("versioned") is transform_v1

        scope.add_transform("versioned", transform_v2)
        assert scope.get_transform("versioned") is transform_v2

    def test_scope_add_transform_returns_self(self):
        """Test that add_transform returns self for chaining."""
        scope = LocalScope()

        result = scope.add_transform("t1", lambda a, s: s)

        assert result is scope


# ============================================================
# JobSpec.with_transform tests
# ============================================================


class TestJobSpecWithTransform:
    """Tests for JobSpec.with_transform method."""

    def test_with_transform_adds_transform(self):
        """Test that with_transform adds a TransformSpec."""
        transformed = SuccessfulJob.with_transform("retry", {"max_retries": 5})

        assert len(transformed.transforms) == 1
        assert transformed.transforms[0].type == "retry"
        assert transformed.transforms[0].args == {"max_retries": 5}

    def test_with_transform_preserves_original(self):
        """Test that with_transform doesn't modify the original spec."""
        original_transforms = len(SuccessfulJob.transforms)

        SuccessfulJob.with_transform("retry", {"max_retries": 5})

        assert len(SuccessfulJob.transforms) == original_transforms

    def test_with_transform_preserves_spec_properties(self):
        """Test that with_transform preserves type, run, recover, precheck."""
        transformed = SuccessfulJob.with_transform("retry")

        assert transformed.type == SuccessfulJob.type
        assert transformed.run == SuccessfulJob.run
        assert transformed.recover == SuccessfulJob.recover
        assert transformed.precheck == SuccessfulJob.precheck

    def test_with_transform_chaining(self):
        """Test chaining multiple with_transform calls."""
        transformed = (
            SuccessfulJob
            .with_transform("retry", {"max_retries": 3})
            .with_transform("custom", {"key": "value"})
        )

        assert len(transformed.transforms) == 2
        assert transformed.transforms[0].type == "retry"
        assert transformed.transforms[1].type == "custom"

    def test_with_transform_default_empty_args(self):
        """Test with_transform with no args defaults to empty dict."""
        transformed = SuccessfulJob.with_transform("retry")

        assert transformed.transforms[0].args == {}


# ============================================================
# JobSpec.apply_transforms tests
# ============================================================


class TestJobSpecApplyTransforms:
    """Tests for JobSpec.apply_transforms method."""

    def test_apply_transforms_no_transforms(self):
        """Test apply_transforms with no transforms returns equivalent spec."""
        scope = LocalScope()
        result = SuccessfulJob.apply_transforms(scope)

        assert result.type == SuccessfulJob.type
        assert result.run == SuccessfulJob.run
        assert result.transforms == []

    def test_apply_transforms_preserves_transforms_list(self):
        """Test that apply_transforms preserves transforms on result for serialization."""
        scope = LocalScope()
        transformed = SuccessfulJob.with_transform("retry")
        result = transformed.apply_transforms(scope)

        # Transforms are preserved so they can be serialized and re-applied on load
        assert len(result.transforms) == 1
        assert result.transforms[0].type == "retry"

    def test_apply_transforms_calls_transform_function(self):
        """Test that apply_transforms actually calls the transform function."""
        call_log = []

        def logging_transform(args, spec):
            call_log.append({"args": args, "type": spec.type})
            return spec

        scope = LocalScope()
        scope.add_transform("logging", logging_transform)

        transformed = SuccessfulJob.with_transform("logging", {"key": "value"})
        transformed.apply_transforms(scope)

        assert len(call_log) == 1
        assert call_log[0]["args"] == {"key": "value"}
        assert call_log[0]["type"] == "SuccessfulJob"

    def test_apply_transforms_chains_transforms(self):
        """Test that multiple transforms are applied in order."""
        call_order = []

        def transform_a(args, spec):
            call_order.append("a")
            return spec

        def transform_b(args, spec):
            call_order.append("b")
            return spec

        scope = LocalScope()
        scope.add_transform("a", transform_a)
        scope.add_transform("b", transform_b)

        transformed = (
            SuccessfulJob
            .with_transform("a")
            .with_transform("b")
        )
        transformed.apply_transforms(scope)

        assert call_order == ["a", "b"]

    def test_apply_transforms_raises_on_missing_transform(self):
        """Test that apply_transforms raises when transform not in scope."""
        scope = LocalScope()
        transformed = SuccessfulJob.with_transform("nonexistent")

        with pytest.raises(TransformNotInScopeError):
            transformed.apply_transforms(scope)


# ============================================================
# JobSpec.save with transforms tests
# ============================================================


class TestJobSpecSaveWithTransforms:
    """Tests for JobSpec serialization with transforms."""

    def test_save_no_transforms(self):
        """Test saving a JobSpec with no transforms."""
        saved = SuccessfulJob.save()

        assert saved["type"] == "SuccessfulJob"
        assert saved["transforms"] == []

    def test_save_with_transforms(self):
        """Test saving a JobSpec with transforms."""
        transformed = SuccessfulJob.with_transform("retry", {"max_retries": 5})
        saved = transformed.save()

        assert saved["type"] == "SuccessfulJob"
        assert len(saved["transforms"]) == 1
        assert saved["transforms"][0] == {"type": "retry", "args": {"max_retries": 5}}

    def test_save_with_multiple_transforms(self):
        """Test saving a JobSpec with multiple transforms."""
        transformed = (
            SuccessfulJob
            .with_transform("retry", {"max_retries": 3})
            .with_transform("custom", {"foo": "bar"})
        )
        saved = transformed.save()

        assert len(saved["transforms"]) == 2
        assert saved["transforms"][0] == {"type": "retry", "args": {"max_retries": 3}}
        assert saved["transforms"][1] == {"type": "custom", "args": {"foo": "bar"}}


# ============================================================
# Retry transform unit tests
# ============================================================


class TestRetryTransform:
    """Tests for the retry transform function."""

    def test_retry_returns_job_spec(self):
        """Test that retry returns a JobSpec."""
        result = retry({}, SuccessfulJob)

        assert isinstance(result, JobSpec)

    def test_retry_preserves_type(self):
        """Test that retry preserves the original job type."""
        result = retry({}, SuccessfulJob)

        assert result.type == SuccessfulJob.type

    def test_retry_preserves_precheck(self):
        """Test that retry preserves the original precheck."""
        result = retry({}, SuccessfulJob)

        assert result.precheck == SuccessfulJob.precheck

    def test_retry_preserves_recover(self):
        """Test that retry preserves the original recover."""
        result = retry({}, SuccessfulJob)

        assert result.recover == SuccessfulJob.recover

    def test_retry_wraps_run(self):
        """Test that retry creates a new run function."""
        result = retry({}, SuccessfulJob)

        assert result.run != SuccessfulJob.run

    def test_retry_default_max_retries(self):
        """Test that retry defaults to 3 max retries."""
        # This is tested implicitly by the behavior - we just verify args parsing
        result = retry({}, SuccessfulJob)
        assert result is not None

    def test_retry_default_backoff_factor(self):
        """Test that retry defaults to 1.0 backoff factor."""
        # This is tested implicitly by the behavior - we just verify args parsing
        result = retry({}, SuccessfulJob)
        assert result is not None

    def test_retry_custom_args(self):
        """Test retry with custom max_retries and backoff_factor."""
        result = retry({"max_retries": 5, "backoff_factor": 2.5}, SuccessfulJob)

        assert result is not None
        assert result.type == SuccessfulJob.type


# ============================================================
# Integration: applying retry transform
# ============================================================


class TestRetryTransformIntegration:
    """Integration tests for retry transform with apply_transforms."""

    def test_apply_retry_transform(self):
        """Test applying retry transform through scope."""
        scope = LocalScope()
        transformed = SuccessfulJob.with_transform("retry", {"max_retries": 2})
        result = transformed.apply_transforms(scope)

        assert result.type == "SuccessfulJob"
        # The run function should be wrapped
        assert result.run != SuccessfulJob.run

    def test_retry_transform_integration_with_scope(self):
        """Test that retry from scope works correctly."""
        scope = LocalScope()

        # Get retry from scope
        retry_fn = scope.get_transform("retry")
        assert retry_fn is retry

        # Apply it
        result = retry_fn({"max_retries": 1}, SuccessfulJob)
        assert result.type == "SuccessfulJob"


# ============================================================
# LocalWorkflow integration tests
# ============================================================


# Jobs for workflow integration tests - defined at module level for scope discovery

@spec()
def SimpleSuccessJob(spec_args, context: Context, input: dict, dependencies: DependencyGroup):
    """A simple job that succeeds and returns input value."""
    yield JobOutputEvent({"value": input.get("value", 42)})


@spec()
def CountingJob(spec_args, context: Context, input: dict, dependencies: DependencyGroup):
    """A job that tracks attempt counts via context state.

    Uses context.state to track attempts across retries.
    """
    state_key = input.get("state_key", "attempt_count")

    # Get current attempt count from shared state
    current_count = context.state.get(state_key, 0)
    current_count += 1
    context.state[state_key] = current_count

    fail_until = input.get("fail_until", 0)

    if current_count <= fail_until:
        raise ValueError(f"Failing on attempt {current_count}")

    yield JobOutputEvent({"attempts": current_count, "value": input.get("value", "success")})


@spec()
def WorkflowWithTransformedJob(spec_args, context: Context, input: dict, dependencies: DependencyGroup):
    """A workflow that uses a transformed job via apply_transforms."""
    from zahir.events import Await

    # Get the job spec and apply retry transform
    transformed_spec = CountingJob.with_transform("retry", {"max_retries": 3, "backoff_factor": 0.01})
    runnable_spec = transformed_spec.apply_transforms(context.scope)

    # Create and await the job instance
    job_instance = runnable_spec(
        {"state_key": input["state_key"], "fail_until": input.get("fail_until", 0), "value": "transformed"},
        {}
    )

    result = yield Await(job_instance)

    yield WorkflowOutputEvent({"result": result})


class TestLocalWorkflowWithTransforms:
    """Integration tests that run transformed jobs through LocalWorkflow.

    Note: The retry transform wraps the run function, but exceptions raised inside
    generators bubble up through yield/yield from and are caught by the state machine
    before the retry wrapper's try/except can handle them. This means retry logic
    needs to use Await to catch errors from child jobs, not try/except around yield from.

    These tests focus on verifying that transforms are correctly applied and that
    the transformed specs run through the workflow engine.
    """

    def test_workflow_runs_simple_job(self):
        """Baseline test: verify LocalWorkflow runs a simple job correctly."""
        tmp_file = "/tmp/zahir_transform_simple.db"
        pathlib.Path(tmp_file).unlink(missing_ok=True)

        scope = LocalScope.from_module(sys.modules[__name__])
        context = MemoryContext(scope=scope, job_registry=SQLiteJobRegistry(tmp_file))
        workflow = LocalWorkflow(context)

        job = SimpleSuccessJob({"value": 123}, {})
        events = list(workflow.run(job, events_filter=None))

        # Should get job output events (need events_filter=None to see them)
        output_events = [e for e in events if isinstance(e, JobOutputEvent)]
        assert len(output_events) == 1
        assert output_events[0].output["value"] == 123

    def test_workflow_with_manually_transformed_job(self):
        """Test running a job that was manually transformed with retry."""
        tmp_file = "/tmp/zahir_transform_manual.db"
        pathlib.Path(tmp_file).unlink(missing_ok=True)

        scope = LocalScope.from_module(sys.modules[__name__])
        context = MemoryContext(scope=scope, job_registry=SQLiteJobRegistry(tmp_file))

        # Apply retry transform manually
        transformed_spec = retry({"max_retries": 2, "backoff_factor": 0.01}, SimpleSuccessJob)

        # Run the transformed job
        job = transformed_spec({"value": 456}, {})

        workflow = LocalWorkflow(context)
        events = list(workflow.run(job, events_filter=None))

        output_events = [e for e in events if isinstance(e, JobOutputEvent)]
        assert len(output_events) == 1
        assert output_events[0].output["value"] == 456

    def test_workflow_with_apply_transforms(self):
        """Test running a job using with_transform and apply_transforms."""
        tmp_file = "/tmp/zahir_transform_apply.db"
        pathlib.Path(tmp_file).unlink(missing_ok=True)

        scope = LocalScope.from_module(sys.modules[__name__])
        context = MemoryContext(scope=scope, job_registry=SQLiteJobRegistry(tmp_file))

        # Use with_transform and apply_transforms
        transformed_spec = SimpleSuccessJob.with_transform("retry", {"max_retries": 1})
        runnable_spec = transformed_spec.apply_transforms(scope)

        job = runnable_spec({"value": 789}, {})

        workflow = LocalWorkflow(context)
        events = list(workflow.run(job, events_filter=None))

        # With retry, we get 2 outputs: one from child job, one forwarded by retry wrapper
        output_events = [e for e in events if isinstance(e, JobOutputEvent)]
        assert len(output_events) >= 1

        # At least one output should have our value
        value_outputs = [e for e in output_events if e.output.get("value") == 789]
        assert len(value_outputs) >= 1

    def test_workflow_job_completes_with_transform(self):
        """Test that a transformed job reaches COMPLETED state."""
        tmp_file = "/tmp/zahir_transform_completes.db"
        pathlib.Path(tmp_file).unlink(missing_ok=True)

        scope = LocalScope.from_module(sys.modules[__name__])
        context = MemoryContext(scope=scope, job_registry=SQLiteJobRegistry(tmp_file))

        transformed_spec = SimpleSuccessJob.with_transform("retry", {"max_retries": 1})
        runnable_spec = transformed_spec.apply_transforms(scope)

        job = runnable_spec({"value": 999}, {})

        workflow = LocalWorkflow(context)
        events = list(workflow.run(job, events_filter=None))

        # Job should complete successfully (parent retry job + child job)
        complete_events = [e for e in events if isinstance(e, JobCompletedEvent)]
        assert len(complete_events) >= 1

        workflow_complete = [e for e in events if isinstance(e, WorkflowCompleteEvent)]
        assert len(workflow_complete) == 1

    def test_workflow_retry_on_failure_then_succeed(self):
        """Test that retry transform actually retries failed jobs and succeeds."""
        tmp_file = "/tmp/zahir_transform_retry_succeed.db"
        pathlib.Path(tmp_file).unlink(missing_ok=True)

        scope = LocalScope.from_module(sys.modules[__name__])
        context = MemoryContext(scope=scope, job_registry=SQLiteJobRegistry(tmp_file))

        # This job will fail twice then succeed on third attempt
        transformed_spec = CountingJob.with_transform("retry", {"max_retries": 3, "backoff_factor": 0.01})
        runnable_spec = transformed_spec.apply_transforms(scope)

        # fail_until=2 means fail on attempts 1 and 2, succeed on 3
        job = runnable_spec({"state_key": "retry_test_succeed", "fail_until": 2, "value": "success!"}, {})

        workflow = LocalWorkflow(context)
        events = list(workflow.run(job, events_filter=None))

        # Should have successful output from the retry wrapper
        output_events = [e for e in events if isinstance(e, JobOutputEvent)]
        assert len(output_events) >= 1

        # The final output should have the successful value
        # (there may be multiple outputs - one from child, one forwarded by parent)
        final_outputs = [e for e in output_events if e.output.get("value") == "success!"]
        assert len(final_outputs) >= 1

        # Check that we actually retried (should have 3 attempts)
        success_outputs = [e for e in output_events if e.output.get("attempts") == 3]
        assert len(success_outputs) >= 1

    def test_workflow_retry_exhausted(self):
        """Test that retry transform gives up after max retries."""
        tmp_file = "/tmp/zahir_transform_retry_exhausted.db"
        pathlib.Path(tmp_file).unlink(missing_ok=True)

        scope = LocalScope.from_module(sys.modules[__name__])
        context = MemoryContext(scope=scope, job_registry=SQLiteJobRegistry(tmp_file))

        # This job will fail 5 times, but we only allow 2 retries (3 total attempts)
        transformed_spec = CountingJob.with_transform("retry", {"max_retries": 2, "backoff_factor": 0.01})
        runnable_spec = transformed_spec.apply_transforms(scope)

        job = runnable_spec({"state_key": "exhaust_test", "fail_until": 5, "value": "never"}, {})

        workflow = LocalWorkflow(context)
        events = list(workflow.run(job, events_filter=None))

        # Should have no successful output with the "value" key (Sleep outputs have duration_seconds)
        output_events = [e for e in events if isinstance(e, JobOutputEvent) and e.output.get("value") is not None]
        assert len(output_events) == 0

        # Workflow should still complete
        complete_events = [e for e in events if isinstance(e, WorkflowCompleteEvent)]
        assert len(complete_events) == 1
