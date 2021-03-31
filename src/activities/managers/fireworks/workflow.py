from __future__ import annotations

import typing

if typing.TYPE_CHECKING:
    from typing import Dict, List, Optional, Sequence, Union
    from uuid import UUID

    from fireworks.core.firework import Firework, Workflow
    from maggma.core import Store

    import activities


def activity_to_workflow(
    activity: Union[activities.Activity, activities.Job, List[activities.Job]],
    store: Store,
) -> Workflow:
    from fireworks.core.firework import Workflow

    from activities.core.activity import Activity

    parent_mapping = {}
    fireworks = []

    if not isinstance(activity, Activity):
        # a list of jobs has been provided; make dummy activity to contain them
        activity = Activity(jobs=activity)

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
    from fireworks.core.firework import Firework

    from activities.core.config import ReferenceFallback
    from activities.managers.fireworks.firetask import JobFiretask

    if (parents is None) is not (parent_mapping is None):
        raise ValueError("Both of neither of parents and parent_mapping must be set.")

    job_firetask = JobFiretask(job=job, store=store)

    job_parents = None
    if parents is not None:
        job_parents = (
            [parent_mapping[parent] for parent in parents] if parents else None
        )

    spec = {"_add_launchpad_and_fw_id": True}  # this allows the job to know the fw_id
    if job.config.on_missing_references != ReferenceFallback.ERROR:
        spec["_allow_fizzled_parents"] = True
    spec.update(job.config.manager_config)

    fw = Firework(tasks=[job_firetask], name=job.name, parents=job_parents, spec=spec)

    if parent_mapping is not None:
        parent_mapping[job.uuid] = fw

    return fw
