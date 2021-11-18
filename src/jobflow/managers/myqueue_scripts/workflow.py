"""Template workflow.py file for running jobflow workflows in myqueue."""


import json

from monty.json import MontyDecoder
from myqueue.task import task


def create_tasks():
    """Create tasks for myqueue."""
    # First reconstruct the jobflow Flow object
    with open("flow.json", "r") as f:
        flow = json.load(f, cls=MontyDecoder)

    tasks = []
    uuid2task = {}
    for job, parents in flow.iterflow():
        deps = [uuid2task[parent_uuid] for parent_uuid in parents]
        t = task(
            "jobflow.managers.myqueue@run_myqueue_task", args=[job.uuid], deps=deps
        )
        uuid2task[job.uuid] = t
        tasks.append(t)

    return tasks
