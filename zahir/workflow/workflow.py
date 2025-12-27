"""Workflow execution engine"""

from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Generic, Iterator, Mapping, TypeVar

from zahir.events import (
    JobRunnableEvent,
    JobRunningEvent,
    WorkflowCompleteEvent,
    WorkflowOutputEvent,
    ZahirEvent,
)
from zahir.types import Context, Job
from zahir.utils.id_generator import generate_id
from zahir.workflow.execute import handle_workflow_stall, execute_workflow_batch

WorkflowOutputType = TypeVar("WorkflowOutputType", bound=Mapping[str, Any])


class Workflow(Generic[WorkflowOutputType]):
    """A workflow execution engine"""

    DEFAULT_MAX_WORKERS = 4
    # How long should we wait between workflow phases? (in seconds)
    # By default, we wait five seconds before rechecking for runnable jobs.s
    STALL_TIME = 5

    def __init__(
        self,
        context: Context,
        max_workers: int | None = None,
        stall_time: int | None = None,
    ) -> None:
        """Initialize a workflow execution engine

        @param context: The context containing scope, job registry, and event registry
        @param max_workers: Maximum number of parallel workers (default: DEFAULT_MAX_WORKERS)
        @param stall_time: Time to wait between workflow phases. Wait times may be larger,
            as this includes the length the jobs themselves run for (default: STALL_TIME)
        """

        self.context = context
        self.max_workers = (
            max_workers if max_workers is not None else self.DEFAULT_MAX_WORKERS
        )
        self.stall_time = stall_time if stall_time is not None else self.STALL_TIME

    def _workflow_id(self) -> str:
        """Generate a unique workflow ID using adjective-noun format"""

        return generate_id(2)

    def _run(self, context: Context, start: Job | None = None) -> Iterator[ZahirEvent]:
        """Run a workflow from the starting job

        @param start: The starting job of the workflow
        @param context: The context to use for job execution (created from scope/registries if not provided)
        """

        workflow_id = self._workflow_id()
        workflow_start_time = datetime.now(tz=timezone.utc)

        # if desired, seed the workflow with an initial job. Otherwise we'll just
        # process jobs currently in the workflow registry.
        if start is not None:
            context.job_registry.add(start)

        with ThreadPoolExecutor(max_workers=self.max_workers) as exec:
            while True:
                # Note: this is a bit memory-inefficient.
                runnable_jobs = list(self.context.job_registry.runnable(self.context))

                # Yield information on each job we currently consider runnable.
                for runnable_job_id, runnable in runnable_jobs:
                    yield JobRunnableEvent(workflow_id, runnable_job_id)

                running_jobs = list(self.context.job_registry.running(self.context))

                # Yield information on each job currently running.
                for running_job_id, running in running_jobs:
                    yield JobRunningEvent(workflow_id, running_job_id)

                # We're finished; record we're done and exit.
                if not runnable_jobs and not running_jobs:
                    workflow_end_time = datetime.now(tz=timezone.utc)
                    workflow_duration = (
                        workflow_end_time - workflow_start_time
                    ).total_seconds()

                    yield WorkflowCompleteEvent(workflow_id, workflow_duration)
                    break

                # Run the batch of jobs that are unblocked across `max_workers` threads.
                batch_start_time = datetime.now(tz=timezone.utc)
                yield from execute_workflow_batch(
                    exec, runnable_jobs, workflow_id, self.context
                )

                batch_end_time = datetime.now(tz=timezone.utc)
                batch_duration = (batch_end_time - batch_start_time).total_seconds()

                # For IO-bound workflows, we're unlikely to need this. But
                # for shorter workflows throttling might be needed.
                yield from handle_workflow_stall(
                    batch_duration, self.stall_time, workflow_id
                )

    def run(
        self, start: Job | None = None
    ) -> Iterator[WorkflowOutputEvent[WorkflowOutputType]]:
        """Run a workflow from the starting job

        @param start: The starting job of the workflow
        """

        for event in self._run(self.context, start):
            self.context.event_registry.register(event)
            self.context.logger.render(self.context)

            if isinstance(event, WorkflowOutputEvent):
                yield event
