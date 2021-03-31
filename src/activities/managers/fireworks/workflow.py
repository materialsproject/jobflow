from __future__ import annotations

import typing

from activities import Job

if typing.TYPE_CHECKING:
    from typing import Union, List, Optional, Dict, Sequence
    from uuid import UUID

    from maggma.core import Store
    from fireworks.core.firework import Workflow, Firework

    import activities


def activity_to_workflow(
    activity: Union[activities.Activity, activities.Job, List[activities.Job]],
    store: Store
) -> Workflow:
    from fireworks.core.firework import Workflow
    from activities.core.activity import Activity

    # TODO: handle activity config and metadata

    parent_mapping = {}
    fireworks = []

    if not isinstance(activity, Activity):
        # a list of jobs has been provided; make dummy activity to contain them
        activity = Activity(jobs=activity)
        print(activity)

    for job, parents in activity.iteractivity():
        fw = job_to_firework(job, store, parents=parents, parent_mapping=parent_mapping)
        fireworks.append(fw)

    return Workflow(fireworks, name=activity.name)


def job_to_firework(
    job: activities.Job,
    store: Store,
    parents: Optional[Sequence[UUID]] = None,
    parent_mapping: Optional[Dict[UUID, Firework]] = None,
):
    from activities.managers.fireworks.firetask import JobFiretask
    from fireworks.core.firework import Firework

    if (parents is None) is not (parent_mapping is None):
        raise ValueError("Both of neither of parents and parent_mapping must be set.")

    job_firetask = JobFiretask(job=job, store=store)
    print("FIRETASK",job_firetask.as_dict())

    if parents is not None:
        job_parents = [parent_mapping[parent] for parent in parents] if parents else None
        fw = Firework(tasks=[job_firetask], parents=job_parents, name=job.name)
        parent_mapping[job.uuid] = fw
    else:
        fw = Firework(tasks=[job_firetask], name=job.name)
        print("FW", fw.as_dict())

    return fw
