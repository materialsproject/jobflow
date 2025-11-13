"""Tools for running jobflow locally."""

from __future__ import annotations

import logging
import typing

if typing.TYPE_CHECKING:
    from pathlib import Path

    import jobflow

logger = logging.getLogger(__name__)


def run_locally(
    flow: jobflow.Flow | jobflow.Job | list[jobflow.Job],
    log: bool | str = True,
    store: jobflow.JobStore | None = None,
    create_folders: bool = False,
    root_dir: str | Path | None = None,
    ensure_success: bool = False,
    allow_external_references: bool = False,
    raise_immediately: bool = False,
) -> dict[str, dict[int, jobflow.Response]]:
    """
    Run a :obj:`Job` or :obj:`Flow` locally.

    Parameters
    ----------
    flow : Flow | Job | list[Job]
        A job or flow.
    log : bool | str
        Controls logging. Defaults to True. Can be:
        - False: disable logging
        - True: use default logging format (read from ~/.jobflow.yaml)
        - str: custom logging format string (e.g. "%(message)s" for more concise output)
    store : JobStore
        A job store. If a job store is not specified then
        :obj:`JobflowSettings.JOB_STORE` will be used. By default this is a maggma
        ``MemoryStore`` but can be customised by setting the jobflow configuration file.
    create_folders : bool
        Whether to run each job in a new folder.
    root_dir : str | Path | None
        The root directory to run the jobs in or where to create new subfolders if
            ``create_folders`` is True. If None then the current working
            directory will be used.
    ensure_success : bool
        Raise an error if the flow was not executed successfully.
    allow_external_references : bool
        If False all the references to other outputs should be from other Jobs
        of the same Flow.
    raise_immediately : bool
        If True, raise an exception immediately if a job fails. If False, continue
        running the flow and only raise an exception at the end if the flow did not
        finish running successfully.

    Returns
    -------
    dict[str, dict[int, Response]]
        The responses of the jobs, as a dict of ``{uuid: {index: response}}``.
    """
    from collections import defaultdict
    from datetime import datetime, timezone
    from pathlib import Path
    from random import randint

    from monty.os import cd

    from jobflow import SETTINGS, initialize_logger
    from jobflow.core.flow import get_flow
    from jobflow.core.reference import OnMissing

    if store is None:
        store = SETTINGS.JOB_STORE

    root_dir = Path.cwd() if root_dir is None else Path(root_dir).resolve()
    root_dir.mkdir(exist_ok=True)

    store.connect()

    if log:
        initialize_logger(fmt=log if isinstance(log, str) else "")

    flow = get_flow(flow, allow_external_references=allow_external_references)

    stopped_parents: set[str] = set()
    errored: set[str] = set()
    responses: dict[str, dict[int, jobflow.Response]] = defaultdict(dict)
    stop_jobflow = False

    def _run_job(job: jobflow.Job, parents):
        nonlocal stop_jobflow

        if stop_jobflow:
            return None, True

        if len(set(parents).intersection(stopped_parents)) > 0:
            # stop children has been called for one of the jobs' parents
            logger.info(
                f"{job.name} is a child of a job with stop_children=True, skipping..."
            )
            stopped_parents.add(job.uuid)
            return None, False

        if (
            len(set(parents).intersection(errored)) > 0
            and job.config.on_missing_references == OnMissing.ERROR
        ):
            errored.add(job.uuid)
            return None, False

        if raise_immediately:
            response = job.run(store=store)
        else:
            try:
                response = job.run(store=store)
            except Exception:
                import traceback

                logger.info(
                    f"{job.name} failed with exception:\n{traceback.format_exc()}"
                )
                errored.add(job.uuid)
                return None, False

        responses[job.uuid][job.index] = response

        if response.stored_data is not None:
            logger.warning("Response.stored_data is not supported with local manager.")

        if response.stop_children:
            stopped_parents.add(job.uuid)

        if response.stop_jobflow:
            stop_jobflow = True
            return None, True

        diversion_responses = []
        if response.replace is not None:
            # first run any restarts
            diversion_responses.append(_run(response.replace))

        if response.detour is not None:
            # next any detours
            diversion_responses.append(_run(response.detour))

        if response.addition is not None:
            # finally any additions
            diversion_responses.append(_run(response.addition))

        if not all(diversion_responses):
            return None, False
        return response, False

    def _get_job_dir():
        if create_folders:
            time_now = datetime.now(tz=timezone.utc).strftime(SETTINGS.DIRECTORY_FORMAT)
            job_dir = root_dir / f"job_{time_now}-{randint(10000, 99999)}"
            job_dir.mkdir()
            return job_dir
        return root_dir

    def _run(root_flow):
        encountered_bad_response = False
        for job, parents in root_flow.iterflow():
            job_dir = _get_job_dir()
            with cd(job_dir):
                response, jobflow_stopped = _run_job(job, parents)

            if response is not None:
                response.job_dir = job_dir
            encountered_bad_response = encountered_bad_response or response is None
            if jobflow_stopped:
                return False

        return not encountered_bad_response

    logger.info("Started executing jobs locally")
    finished_successfully = _run(flow)
    logger.info("Finished executing jobs locally")

    if ensure_success and not finished_successfully:
        raise RuntimeError("Flow did not finish running successfully")

    return dict(responses)
