"""Tools for running jobflow locally."""

from __future__ import annotations

import logging
import typing

if typing.TYPE_CHECKING:
    from typing import Dict, List, Optional, Union

    import jobflow

__all__ = ["run_locally"]

logger = logging.getLogger(__name__)


def run_locally(
    flow: Union[jobflow.Flow, jobflow.Job, List[jobflow.Job]],
    log: bool = True,
    store: Optional[jobflow.JobStore] = None,
    queue: Optional[jobflow.Queue] = None,
    create_folders: bool = False,
    write_jobfile: bool = True,
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
    from pathlib import Path

    from monty.os import cd
    from monty.serialization import dumpfn

    from jobflow import SETTINGS, Queue, initialize_logger
    from jobflow.core.flow import get_flow

    if store is None:
        store = SETTINGS.JOB_STORE

    if queue is None:
        queue = Queue()

    store.connect()

    if log:
        initialize_logger()

    flow = get_flow(flow)
    queue.add_flow(flow)

    logger.info("Started executing jobs locally")
    responses: Dict[str, dict] = defaultdict(dict)
    root_dir = Path.cwd()
    while True:
        if create_folders:
            launch_dir = _get_launch_dir(root_dir)
        else:
            launch_dir = root_dir

        job = queue.checkout_job(flow_uuid=flow.uuid, launch_dir=str(launch_dir))

        if job is None:
            break

        if launch_dir != root_dir:
            launch_dir.mkdir()

        with cd(launch_dir):
            if write_jobfile and create_folders:
                dumpfn(job, "job.json")

            try:
                response = job.run(store=store)
            except Exception:
                import traceback

                logger.info(
                    f"{job.name} failed with exception:\n{traceback.format_exc()}"
                )
                response = None

            responses[job.uuid][job.index] = response
            queue.checkin_job(job, response)

    logger.info("Finished executing jobs locally")

    jobs = queue.get_jobs_info_by_flow_uuid(flow.uuid, ["state"])
    finished_successfully = all([job["state"] == "completed" for job in jobs])

    if ensure_success and not finished_successfully:
        raise RuntimeError("Flow did not finish running successfully")

    return dict(responses)


def _get_launch_dir(root_dir):
    from datetime import datetime
    from random import randint

    time_now = datetime.utcnow().strftime("%Y-%m-%d-%H-%M-%S-%f")
    return root_dir / f"job_{time_now}-{randint(10000, 99999)}"
