"""Tools for running jobflow locally."""

from __future__ import annotations

import logging
import typing

if typing.TYPE_CHECKING:
    from typing import Dict, List, Optional, Set, Union

    import jobflow

__all__ = ["run_locally"]

logger = logging.getLogger(__name__)


def run_locally(
    flow: Union[jobflow.Flow, jobflow.Job, List[jobflow.Job]],
    log: bool = True,
    store: Optional[jobflow.JobStore] = None,
    create_folders: bool = False,
    ensure_success: bool = False,
) -> Dict[str, Dict[int, jobflow.Response]]:
    """
    Run a :obj:`Job` or :obj:`Flow` locally.

    Parameters
    ----------
    flow
        A job or flow.
    log
        Whether to print log messages.
    store
        A job store. If a job store is not specified then
        :obj:`JobflowSettings.JOB_STORE` will be used. By default this is a maggma
        ``MemoryStore`` but can customised by setting the jobflow configuration file.
    create_folders
        Whether to run each job in a new folder.
    ensure_success
        Raise an error if the flow was not executed successfully.

    Returns
    -------
    Dict[str, Dict[int, Response]]
        The responses of the jobs, as a dict of ``{uuid: {index: response}}``.
    """
    from collections import defaultdict
    from datetime import datetime
    from pathlib import Path
    from random import randint

    from monty.os import cd

    from jobflow import SETTINGS, Job, initialize_logger
    from jobflow.core.flow import get_flow
    from jobflow.core.reference import OnMissing

    if store is None:
        store = SETTINGS.JOB_STORE

    store.connect()

    if log:
        initialize_logger()

    flow = get_flow(flow)

    stopped_parents: Set[str] = set()
    errored: Set[str] = set()
    responses: Dict[str, Dict[int, jobflow.Response]] = defaultdict(dict)
    stop_jobflow = False

    root_dir = Path.cwd()

    def _run_job(job: jobflow.Job, parents):
        nonlocal stop_jobflow

        if stop_jobflow:
            return False

        if len(set(parents).intersection(stopped_parents)) > 0:
            # stop children has been called for one of the jobs' parents
            logger.info(
                f"{job.name} is a child of a job with stop_children=True, skipping..."
            )
            stopped_parents.add(job.uuid)
            return

        if (
            len(set(parents).intersection(errored)) > 0
            and job.config.on_missing_references == OnMissing.ERROR
        ):
            errored.add(job.uuid)
            return

        try:
            response = job.run(store=store)
        except Exception:
            import traceback

            logger.info(f"{job.name} failed with exception:\n{traceback.format_exc()}")
            errored.add(job.uuid)
            return

        responses[job.uuid][job.index] = response

        if response.stored_data is not None:
            logger.warning("Response.stored_data is not supported with local manager.")

        if response.stop_children:
            stopped_parents.add(job.uuid)

        if response.stop_jobflow:
            stop_jobflow = True
            return False

        if response.replace is not None:
            # first run any restarts
            _run(response.replace)

        if response.detour is not None:
            # next any detours
            _run(response.detour)

        if response.addition is not None:
            # finally any additions
            _run(response.addition)

        return response

    def _get_job_dir():
        if create_folders:
            time_now = datetime.utcnow().strftime("%Y-%m-%d-%H-%M-%S-%f")
            job_dir = root_dir / f"job_{time_now}-{randint(10000, 99999)}"
            job_dir.mkdir()
            return job_dir
        else:
            return root_dir

    def _run(root_flow):
        if isinstance(root_flow, Job):
            job_dir = _get_job_dir()
            with cd(job_dir):
                response = _run_job(root_flow, [])

            if response is False:
                return False

        else:
            job: jobflow.Job
            for job, parents in root_flow.iterflow():
                job_dir = _get_job_dir()
                with cd(job_dir):
                    response = _run_job(job, parents)
                if response is False:
                    return False

        return True if response is not None else False

    logger.info("Started executing jobs locally")
    finished_successfully = _run(flow)
    logger.info("Finished executing jobs locally")

    if ensure_success and not finished_successfully:
        raise RuntimeError("Flow did not finish running successfully")

    return dict(responses)
