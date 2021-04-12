from __future__ import annotations

import typing

from fireworks import FiretaskBase, Firework, FWAction, Workflow, explicit_serialize

if typing.TYPE_CHECKING:
    from typing import Dict, List, Optional, Sequence, Union

    import jobflow

__all__ = ["flow_to_workflow", "job_to_firework", "JobFiretask"]


def flow_to_workflow(
    flow: Union[jobflow.Flow, jobflow.Job, List[jobflow.Job]],
    store: jobflow.JobStore,
) -> Workflow:
    from fireworks.core.firework import Workflow

    from jobflow.core.flow import Flow

    parent_mapping = {}
    fireworks = []

    if not isinstance(flow, Flow):
        # a list of jobs has been provided; make dummy flow to contain them
        flow = Flow(jobs=flow)

    for job, parents in flow.iterflow():
        fw = job_to_firework(job, store, parents=parents, parent_mapping=parent_mapping)
        fireworks.append(fw)

    return Workflow(fireworks, name=flow.name)


def job_to_firework(
    job: jobflow.Job,
    store: jobflow.JobStore,
    parents: Optional[Sequence[str]] = None,
    parent_mapping: Optional[Dict[str, Firework]] = None,
):
    from fireworks.core.firework import Firework

    from jobflow.core.reference import ReferenceFallback

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
        from jobflow import initialize_logger
        from jobflow.core.job import Job

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
            detours = [flow_to_workflow(response.restart, store)]

        if response.addition is not None:
            additions = [flow_to_workflow(response.addition, store)]

        if response.detour is not None:
            detour_wf = flow_to_workflow(response.detour, store)
            if detours is not None:
                detours.append(detour_wf)
            else:
                detours = [detour_wf]

        return FWAction(
            stored_data=response.stored_data,
            detours=detours,
            additions=additions,
            defuse_workflow=response.stop_flows,
            defuse_children=response.stop_children,
        )
