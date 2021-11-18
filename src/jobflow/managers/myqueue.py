"""Tools for running :obj:`Flow` and :obj:`Job` objects using the Myqueue package.

Notes
-----
Myqueue heavily relies on the file system. To submit a workflow, one has to run:
mq workflow workflow.py DIRECTORY_PATTERNS
where workflow.py is a python script defining one workflow. For jobflow Flows, the
workflow.py file in myqueue_scripts has to be used.
"""

from __future__ import annotations

import json
import os
import typing
from datetime import datetime
from pathlib import Path
from random import randint

from monty.json import MontyDecoder, MontyEncoder
from monty.os import cd

if typing.TYPE_CHECKING:
    from typing import List, Union

    import jobflow

from maggma.stores import JSONStore

from jobflow import JobStore

__all__ = ["flow_to_myqueue", "run_myqueue_task"]

FLOW_JSON = "flow.json"
JOB_STORE_JSON = "job_store.json"


def flow_to_myqueue(
    flow: Union[jobflow.Flow, jobflow.Job, List[jobflow.Job]],
):
    """
    Convert a jobflow Flow to myqueue.

    This is basically just dumping the jobflow Flow to a flow.json file.
    The flow.json file is then read again when the user wants to submit
    the workflow using myqueue.

    Parameters
    ----------
    flow
        A flow or job.

    """
    from jobflow.core.flow import get_flow

    flow = get_flow(flow)
    with open(FLOW_JSON, "w") as f:
        json.dump(flow, f, cls=MontyEncoder, indent=2)


def run_myqueue_task(uuid):
    """
    Run a job in myqueue.

    Parameters
    ----------
    uuid
        Unique identifier of the job that needs to be executed.
    """
    root_dir = Path.cwd()
    # First get the jobflow Flow from the flow.json file
    with open("flow.json", "r") as f:
        flow = json.load(f, cls=MontyDecoder)

    # Get the jobflow Job corresponding to the uuid
    job = _get_job(flow, uuid)
    job_dir = _get_job_dir(root_dir=root_dir)

    # Initialize the store for output references
    job_store_json_path = os.path.join(root_dir, JOB_STORE_JSON)
    if not os.path.exists(job_store_json_path):
        with open(job_store_json_path, "w") as f:
            json.dump([], f)
    store = JobStore(JSONStore(job_store_json_path, writable=True))
    store.connect()

    # Run the job
    with cd(job_dir):
        job.run(store=store)


def _get_job(flow, uuid):
    myjob = None
    for job, _ in flow.iterflow():
        if job.uuid == uuid:
            if myjob is not None:
                raise RuntimeError(f"Multiple jobs with uuid {uuid}")
            myjob = job
    return myjob


def _get_job_dir(root_dir):
    time_now = datetime.utcnow().strftime("%Y-%m-%d-%H-%M-%S-%f")
    job_dir = root_dir / f"job_{time_now}-{randint(10000, 99999)}"
    job_dir.mkdir()
    return job_dir


if __name__ == "__main__":
    pass
