from __future__ import annotations

import typing

from fireworks import FiretaskBase, Firework, FWAction, Workflow, explicit_serialize

if typing.TYPE_CHECKING:
    from typing import Dict, List, Optional, Sequence, Union

    import activities

__all__ = ["activity_to_workflow", "job_to_firework", "JobFiretask"]


def activity_to_workflow(
    activity: Union[activities.Activity, activities.Job, List[activities.Job]],
    store: activities.ActivityStore,
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
    store: activities.ActivityStore,
    parents: Optional[Sequence[str]] = None,
    parent_mapping: Optional[Dict[str, Firework]] = None,
):
    from fireworks.core.firework import Firework

    from activities.core.reference import ReferenceFallback

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


@explicit_serialize
class JobFiretask(FiretaskBase):

    required_params = ["job", "store"]

    def run_task(self, fw_spec):
        from activities import initialize_logger
        from activities.core.job import Job

        job: Job = self.get("job")
        store = self.get("store")
        store.connect()

        if "fw_id" in fw_spec:
            job.metadata.update({"fw_id": fw_spec["fw_id"]})

        initialize_logger()
        response = job.run(store=store)

        detours = None
        additions = None
        if response.restart is not None:
            # create a workflow from the new additions
            detours = [activity_to_workflow(response.restart, store)]

        if response.addition is not None:
            additions = [activity_to_workflow(response.addition, store)]

        if response.detour is not None:
            detour_wf = activity_to_workflow(response.detour, store)
            if detours is not None:
                detours.append(detour_wf)
            else:
                detours = [detour_wf]

        return FWAction(
            stored_data=response.stored_data,
            detours=detours,
            additions=additions,
            defuse_workflow=response.stop_activities,
            defuse_children=response.stop_children,
        )
