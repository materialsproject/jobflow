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
        A job store. If a job store is not specified then a maggma ``MemoryStore`` will
        be used while the job is executed and deleted once the job is finished.
    create_folders
        Whether to run each job in a new folder.

    Returns
    -------
    Dict[str, Dict[int, Response]]
        The responses of the jobs, as a dict of ``{uuid: {index: response}}``.
    """
    from collections import defaultdict
    from datetime import datetime
    from pathlib import Path
    from random import randint

    from maggma.stores import MemoryStore
    from monty.os import cd

    from jobflow import Job, JobStore, initialize_logger
    from jobflow.core.flow import get_flow
    from jobflow.core.reference import OnMissing

    if store is None:
        jobstore = JobStore(MemoryStore())
        jobstore.connect()
    else:
        jobstore = store
        jobstore.connect()

    if log:
        initialize_logger()

    flow = get_flow(flow)

    stopped_parents: Set[str] = set()
    fizzled: Set[str] = set()
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
            len(set(parents).intersection(fizzled)) > 0
            and job.config.on_missing_references == OnMissing.ERROR
        ):
            fizzled.add(job.uuid)
            return

        try:
            response = job.run(store=jobstore)
        except Exception:
            import traceback

            logger.info(f"{job.name} failed with exception:\n{traceback.format_exc()}")
            fizzled.add(job.uuid)
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

    logger.info("Started executing jobs locally")
    _run(flow)
    logger.info("Finished executing jobs locally")
    return dict(responses)
