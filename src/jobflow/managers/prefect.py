"""Tools for running :obj:`Flow` and :obj:`Job` objects using the Prefect package."""

from __future__ import annotations

import logging
import typing
from typing import Any, Dict, List, Optional, Union

from monty.json import jsanitize

if typing.TYPE_CHECKING:
    from collections.abc import Sequence
    import jobflow
    from jobflow.core.job import Job

try:
    from prefect import flow, task
    from prefect.task_runners import ConcurrentTaskRunner, TaskRunner
    PREFECT_AVAILABLE = True
except ImportError:
    PREFECT_AVAILABLE = False

logger = logging.getLogger(__name__)


class PrefectResultStore:
    """A simple in-memory store for Prefect task results that mimics jobflow store interface."""

    def __init__(self):
        self.results = {}

    def update(self, doc, key=None):
        """Store a result document."""
        self.results[doc.uuid] = doc

    def query_one(self, criteria, fields=None, sort=None):
        """Query for a single document."""
        uuid = criteria.get("uuid")
        if uuid in self.results:
            return self.results[uuid]
        return None

    def query(self, criteria, fields=None, sort=None):
        """Query for documents."""
        uuid = criteria.get("uuid")
        if uuid in self.results:
            yield self.results[uuid]


def flow_to_prefect_flow(
    jobflow_obj: jobflow.Flow | jobflow.Job | Sequence[jobflow.Job],
    store: jobflow.JobStore | None = None,
    allow_external_references: bool = False,
    task_runner: TaskRunner | None = ConcurrentTaskRunner,
    flow_name: str | None = None,
    **kwargs
) -> Any:
    """
    Convert a jobflow Flow or Job to a Prefect flow.

    Parameters
    ----------
    jobflow_obj
        A jobflow Flow, Job, or list of Jobs.
    store
        A job store. If None, JobflowSettings.JOB_STORE will be used.
    allow_external_references
        If False, all references should be from other Jobs in the Flow.
    task_runner
        The task runner to use.
    flow_name
        Name for the Prefect flow. If None, will use the jobflow object's name.
    **kwargs
        Additional keyword arguments passed to the Prefect flow decorator.

    Returns
    -------
    Prefect flow function
        A Prefect flow that can be executed or deployed.
    """
    if not PREFECT_AVAILABLE:
        raise ImportError(
            "Prefect is not installed. Please install it with: pip install prefect"
        )

    from jobflow.core.flow import get_flow

    # Convert to a Flow object if needed
    flow_obj = get_flow(jobflow_obj, allow_external_references=allow_external_references)

    # Determine task runner
    if isinstance(task_runner, str):
        if task_runner == "concurrent":
            runner = ConcurrentTaskRunner()
        elif task_runner == "sequential":
            runner = None  # Default sequential execution in Prefect 3.x
        else:
            runner = ConcurrentTaskRunner()  # default
    else:
        # Assume it's a TaskRunner instance (or None)
        runner = task_runner

    # Set flow name
    if flow_name is None:
        flow_name = getattr(flow_obj, 'name', 'jobflow_prefect_flow')

    @flow(
        name=flow_name,
        task_runner=runner,
        log_prints=True,
        **kwargs
    )
    def prefect_flow():
        """Execute the jobflow Flow/Job as a Prefect flow."""
        from jobflow import SETTINGS

        # Create a Prefect-native result store to handle dependencies
        prefect_store = PrefectResultStore()

        # Create tasks for each job in the flow
        task_results = {}
        job_tasks = {}

        # Convert each job to a Prefect task that can handle references
        for job, parents in flow_obj.iterflow():
            job_task = job_to_prefect_task_with_references(job, prefect_store)
            job_tasks[job.uuid] = job_task

            # Handle dependencies by passing parent task results with UUID mapping
            parent_data = []
            if parents:
                for parent_uuid in parents:
                    if parent_uuid in task_results:
                        parent_data.append({
                            'uuid': parent_uuid,
                            'task_result': task_results[parent_uuid]
                        })

            # Execute the task with parent data
            task_result = job_task.submit(
                job=job,
                store=prefect_store,
                parent_data=parent_data
            )

            task_results[job.uuid] = task_result

        # Return results from all tasks
        return {uuid: result.result() for uuid, result in task_results.items()}

    return prefect_flow


def job_to_prefect_task(
    job: "jobflow.Job",
    store: Optional["jobflow.JobStore"] = None,
    **kwargs
) -> Any:
    """
    Convert a jobflow Job to a Prefect task.

    Parameters
    ----------
    job
        A jobflow Job object.
    store
        A job store to use for the task.
    **kwargs
        Additional keyword arguments passed to the Prefect task decorator.

    Returns
    -------
    Prefect task
        A Prefect task that can execute the jobflow Job.
    """
    if not PREFECT_AVAILABLE:
        raise ImportError(
            "Prefect is not installed. Please install it with: pip install prefect"
        )

    task_name = getattr(job, 'name', f'job_{job.uuid[:8]}')

    @task(
        name=task_name,
        log_prints=True,
        **kwargs
    )
    def execute_job(job: "jobflow.Job", store: "jobflow.JobStore"):
        """Execute a single jobflow Job."""
        from jobflow.core.job import Job
        from jobflow.core.reference import resolve_references
        from copy import deepcopy

        logger.info(f"Executing job: {job.name} ({job.uuid})")

        try:
            # Create a copy of the job to avoid "already belongs to flow" issues
            job_copy = deepcopy(job)

            # Resolve references in the job inputs if needed
            resolved_args = job_copy.function_args
            resolved_kwargs = job_copy.function_kwargs

            if hasattr(job_copy, 'config') and job_copy.config.resolve_references:
                from jobflow.core.reference import find_and_get_references

                # Only resolve if there are actual references
                if job_copy.function_args:
                    args_refs = find_and_get_references(job_copy.function_args)
                    if args_refs:
                        resolved_args = resolve_references(
                            job_copy.function_args,
                            store,
                            job_copy.config.on_missing_references
                        )

                if job_copy.function_kwargs:
                    kwargs_refs = find_and_get_references(job_copy.function_kwargs)
                    if kwargs_refs:
                        resolved_kwargs = resolve_references(
                            job_copy.function_kwargs,
                            store,
                            job_copy.config.on_missing_references
                        )

            # Execute the job function directly
            result = job_copy.function(*resolved_args, **resolved_kwargs)

            # Store the result if store is provided
            if store is not None:
                from jobflow.core.schemas import JobStoreDocument
                doc = JobStoreDocument(
                    uuid=job_copy.uuid,
                    index=1,
                    job=job_copy,
                    output=result
                )
                try:
                    store.update(doc, key=["uuid", "index"])
                except Exception as store_error:
                    logger.warning(f"Failed to store job result: {store_error}")
                    # Continue execution even if storage fails

            logger.info(f"Job {job.name} completed successfully")
            return result

        except Exception as e:
            logger.error(f"Job {job.name} failed: {str(e)}")
            raise

    return execute_job


def job_to_prefect_task_with_references(
    job: "jobflow.Job",
    store: Optional[PrefectResultStore] = None,
    **kwargs
) -> Any:
    """
    Convert a jobflow Job to a Prefect task that can resolve references using Prefect task results.

    Parameters
    ----------
    job
        A jobflow Job object.
    store
        A PrefectResultStore to use for the task.
    **kwargs
        Additional keyword arguments passed to the Prefect task decorator.

    Returns
    -------
    Prefect task
        A Prefect task that can execute the jobflow Job with reference resolution.
    """
    if not PREFECT_AVAILABLE:
        raise ImportError(
            "Prefect is not installed. Please install it with: pip install prefect"
        )

    task_name = getattr(job, 'name', f'job_{job.uuid[:8]}')

    @task(
        name=task_name,
        log_prints=True,
        **kwargs
    )
    def execute_job_with_references(job: "jobflow.Job", store: PrefectResultStore, parent_data: list = None):
        """Execute a single jobflow Job with Prefect-native reference resolution."""
        from jobflow.core.reference import find_and_get_references
        from jobflow.core.schemas import JobStoreDocument
        from copy import deepcopy

        logger.info(f"Executing job: {job.name} ({job.uuid})")

        try:
            # Create a copy of the job to avoid "already belongs to flow" issues
            job_copy = deepcopy(job)

            # Build a UUID -> result mapping from parent data
            result_map = {}
            if parent_data:
                for parent_info in parent_data:
                    parent_uuid = parent_info['uuid']
                    task_result = parent_info['task_result']
                    # Get the actual result value from the Prefect task
                    result_value = task_result.result() if hasattr(task_result, 'result') else task_result
                    result_map[parent_uuid] = result_value
                    logger.info(f"Parent result for {parent_uuid[:8]}: {result_value}")

            # Resolve references in the job inputs using the result mapping
            resolved_args = list(job_copy.function_args) if job_copy.function_args else []
            resolved_kwargs = dict(job_copy.function_kwargs) if job_copy.function_kwargs else {}

            # Check for references and resolve them using parent task results
            if job_copy.function_args:
                args_refs = find_and_get_references(job_copy.function_args)
                if args_refs:
                    resolved_args = resolve_references_with_uuid_mapping(
                        job_copy.function_args, result_map, args_refs
                    )

            if job_copy.function_kwargs:
                kwargs_refs = find_and_get_references(job_copy.function_kwargs)
                if kwargs_refs:
                    resolved_kwargs = resolve_references_with_uuid_mapping(
                        job_copy.function_kwargs, result_map, kwargs_refs
                    )

            # Execute the job function directly
            result = job_copy.function(*resolved_args, **resolved_kwargs)

            # Store the result in our Prefect store
            if store is not None:
                doc = JobStoreDocument(
                    uuid=job_copy.uuid,
                    index=1,
                    job=job_copy,
                    output=result
                )
                try:
                    store.update(doc, key=["uuid", "index"])
                except Exception as store_error:
                    logger.warning(f"Failed to store job result: {store_error}")

            logger.info(f"Job {job.name} completed successfully")
            return result

        except Exception as e:
            logger.error(f"Job {job.name} failed: {str(e)}")
            raise

    return execute_job_with_references


def resolve_references_with_uuid_mapping(data, result_map, references):
    """
    Resolve jobflow references using a UUID -> result mapping.

    This function recursively walks through the data structure and replaces
    OutputReference objects with their corresponding results from the mapping.
    """
    from copy import deepcopy

    def replace_references(obj):
        """Recursively replace references in a data structure."""
        if hasattr(obj, 'uuid') and hasattr(obj, 'attributes'):
            # This is an OutputReference object
            ref_uuid = obj.uuid
            if ref_uuid in result_map:
                result_value = result_map[ref_uuid]

                # Handle attribute access (e.g., job.output.some_attr)
                if obj.attributes:
                    for attr in obj.attributes:
                        if hasattr(result_value, attr):
                            result_value = getattr(result_value, attr)
                        elif isinstance(result_value, dict) and attr in result_value:
                            result_value = result_value[attr]
                        else:
                            logger.warning(f"Attribute {attr} not found in result")

                return result_value
            else:
                logger.warning(f"No result found for reference UUID {ref_uuid}")
                return obj
        elif isinstance(obj, (list, tuple)):
            return type(obj)(replace_references(item) for item in obj)
        elif isinstance(obj, dict):
            return {key: replace_references(value) for key, value in obj.items()}
        else:
            return obj

    return replace_references(data)


class PrefectManager:
    """
    A manager for running jobflow workflows using Prefect.

    This class provides methods to convert jobflow Flows and Jobs
    to Prefect workflows and deploy them to a Prefect server.
    """

    def __init__(
        self,
        store: Optional["jobflow.JobStore"] = None,
        task_runner: Union["TaskRunner", str] = "concurrent",
        client: Optional[Any] = None
    ):
        """
        Initialize the PrefectManager.

        Parameters
        ----------
        store
            Default job store to use for all workflows.
        task_runner
            Default task runner. Can be a TaskRunner instance or a string.
            String options: "concurrent" or "sequential".
        client
            Prefect client instance. If None, will create one automatically.
        """
        if not PREFECT_AVAILABLE:
            raise ImportError(
                "Prefect is not installed. Please install it with: pip install prefect"
            )

        self.store = store
        self.task_runner = task_runner
        self.client = client

    async def submit_flow(
        self,
        jobflow_obj: Union["jobflow.Flow", "jobflow.Job", List["jobflow.Job"]],
        flow_name: Optional[str] = None,
        **kwargs
    ) -> str:
        """
        Submit a jobflow Flow/Job to Prefect for execution.

        Parameters
        ----------
        jobflow_obj
            The jobflow Flow, Job, or list of Jobs to execute.
        flow_name
            Name for the flow. If None, will be auto-generated.
        **kwargs
            Additional arguments passed to flow conversion.

        Returns
        -------
        str
            The flow run ID from Prefect.
        """
        # Convert to Prefect flow
        prefect_flow_func = flow_to_prefect_flow(
            jobflow_obj,
            store=self.store,
            task_runner=self.task_runner,
            flow_name=flow_name,
            **kwargs
        )

        # Execute the flow
        flow_run = prefect_flow_func()
        return flow_run

    def create_deployment(
        self,
        jobflow_obj: Union["jobflow.Flow", "jobflow.Job", List["jobflow.Job"]],
        deployment_name: str,
        work_pool_name: Optional[str] = None,
        **kwargs
    ) -> Any:
        """
        Create a Prefect deployment for a jobflow workflow.

        Note: In Prefect 3.x, use flow.deploy() method instead.

        Parameters
        ----------
        jobflow_obj
            The jobflow Flow, Job, or list of Jobs to deploy.
        deployment_name
            Name for the deployment.
        work_pool_name
            Name of the work pool to use.
        **kwargs
            Additional arguments passed to deployment creation.

        Returns
        -------
        Any
            The deployment result from flow.deploy().
        """
        # Convert to Prefect flow
        prefect_flow_func = flow_to_prefect_flow(
            jobflow_obj,
            store=self.store,
            task_runner=self.task_runner,
            **kwargs
        )

        # Create deployment using Prefect 3.x API
        return prefect_flow_func.deploy(
            name=deployment_name,
            work_pool_name=work_pool_name,
            **kwargs
        )


def run_on_prefect(
    jobflow_obj: Union["jobflow.Flow", "jobflow.Job", List["jobflow.Job"]],
    store: Optional["jobflow.JobStore"] = None,
    task_runner: Union["TaskRunner", str] = "concurrent",
    flow_name: Optional[str] = None,
    **kwargs
) -> Any:
    """
    Run a jobflow Flow/Job on Prefect.

    This is a convenience function that creates and executes a Prefect flow
    from a jobflow object.

    Parameters
    ----------
    jobflow_obj
        The jobflow Flow, Job, or list of Jobs to execute.
    store
        Job store to use. If None, will use JobflowSettings.JOB_STORE.
    task_runner
        Task runner to use. Can be a TaskRunner instance or a string.
        String options: "concurrent" or "sequential".
    flow_name
        Name for the Prefect flow.
    **kwargs
        Additional arguments passed to flow creation.

    Returns
    -------
    Any
        The result of the flow execution.
    """
    if not PREFECT_AVAILABLE:
        raise ImportError(
            "Prefect is not installed. Please install it with: pip install prefect"
        )

    # Create and run the Prefect flow
    prefect_flow_func = flow_to_prefect_flow(
        jobflow_obj,
        store=store,
        task_runner=task_runner,
        flow_name=flow_name,
        **kwargs
    )

    # Execute the flow
    return prefect_flow_func()