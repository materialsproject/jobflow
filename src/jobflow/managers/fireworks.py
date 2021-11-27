"""Tools for running :obj:`Flow` and :obj:`Job` objects using the FireWorks package."""

from __future__ import annotations

import typing

from fireworks import FiretaskBase, Firework, FWAction, Workflow, explicit_serialize

if typing.TYPE_CHECKING:
    from typing import Dict, List, Optional, Sequence, Union

    import jobflow

__all__ = ["flow_to_workflow", "job_to_firework", "JobFiretask"]


def flow_to_workflow(
    flow: Union[jobflow.Flow, jobflow.Job, List[jobflow.Job]],
    store: Optional[jobflow.JobStore] = None,
    **kwargs,
) -> Workflow:
    """
    Convert a :obj:`Flow` or a :obj:`Job` to a FireWorks :obj:`Workflow` object.

    Each firework spec is updated with the contents of the
    :obj:`Job.config.manager_config` dictionary. Accordingly, a :obj:`.JobConfig` object
    can be used to configure FireWork options such as metadata and the fireworker.

    Parameters
    ----------
    flow
        A flow or job.
    store
        A job store. Alternatively, if set to None, :obj:`JobflowSettings.JOB_STORE`
        will be used. Note, this could be different on the computer that submits the
        workflow and the computer which runs the workflow. The value of ``JOB_STORE`` on
        the computer that runs the workflow will be used.
    **kwargs
        Keyword arguments passed to Workflow init method.

    Returns
    -------
    Workflow
        The job or flow as a workflow.
    """
    from fireworks.core.firework import Firework, Workflow

    from jobflow.core.flow import get_flow

    parent_mapping: Dict[str, Firework] = {}
    fireworks = []

    flow = get_flow(flow)

    for job, parents in flow.iterflow():
        fw = job_to_firework(job, store, parents=parents, parent_mapping=parent_mapping)
        fireworks.append(fw)

    return Workflow(fireworks, name=flow.name, **kwargs)


def job_to_firework(
    job: jobflow.Job,
    store: Optional[jobflow.JobStore] = None,
    parents: Optional[Sequence[str]] = None,
    parent_mapping: Optional[Dict[str, Firework]] = None,
    **kwargs,
) -> Firework:
    """
    Convert a :obj:`Job` to a :obj:`.Firework`.

    The firework spec is updated with the contents of the
    :obj:`Job.config.manager_config` dictionary. Accordingly, a :obj:`.JobConfig` object
    can be used to configure FireWork options such as metadata and the fireworker.

    Parameters
    ----------
    job
        A job.
    store
        A job store. Alternatively, if set to None, :obj:`JobflowSettings.JOB_STORE`
        will be used. Note, this could be different on the computer that submits the
        workflow and the computer which runs the workflow. The value of ``JOB_STORE`` on
        the computer that runs the workflow will be used.
    parents
        The parent uuids of the job.
    parent_mapping
        A dictionary mapping job uuids to Firework objects, as ``{uuid: Firework}``.
    **kwargs
        Keyword arguments passed to the Firework constructor.

    Returns
    -------
    Firework
        A firework that will run the job.
    """
    from fireworks.core.firework import Firework

    from jobflow.core.reference import OnMissing

    if (parents is None) is not (parent_mapping is None):
        raise ValueError("Both or neither of parents and parent_mapping must be set.")

    task = JobFiretask(job=job, store=store)

    job_parents = None
    if parents is not None and parent_mapping is not None:
        job_parents = (
            [parent_mapping[parent] for parent in parents] if parents else None
        )

    spec = {"_add_launchpad_and_fw_id": True}  # this allows the job to know the fw_id
    if job.config.on_missing_references != OnMissing.ERROR:
        spec["_allow_fizzled_parents"] = True
    spec.update(job.config.manager_config)
    spec.update(job.metadata)  # add metadata to spec

    fw = Firework([task], spec=spec, name=job.name, parents=job_parents, **kwargs)

    if parent_mapping is not None:
        parent_mapping[job.uuid] = fw

    return fw


@explicit_serialize
class JobFiretask(FiretaskBase):
    """
    A firetask that will run any job.

    Other Parameters
    ----------------
    job : Dict
        A serialized job.
    store : JobStore
        A job store. Alternatively, if set to None, :obj:`JobflowSettings.JOB_STORE`
        will be used. Note, this could be different on the computer that submits the
        workflow and the computer which runs the workflow. The value of ``JOB_STORE`` on
        the computer that runs the workflow will be used.
    """

    required_params = ["job", "store"]

    def run_task(self, fw_spec):
        """Run the job and handle any dynamic firework submissions."""
        from jobflow import SETTINGS, initialize_logger
        from jobflow.core.job import Job

        job: Job = self.get("job")
        store = self.get("store")

        if store is None:
            store = SETTINGS.JOB_STORE
        store.connect()

        if hasattr(self, "fw_id"):
            job.metadata.update({"fw_id": self.fw_id})

        initialize_logger()
        response = job.run(store=store)

        detours = None
        additions = None
        if response.replace is not None:
            # create a workflow from the new additions; be sure to use original store
            detours = [flow_to_workflow(response.replace, self.get("store"))]

        if response.addition is not None:
            additions = [flow_to_workflow(response.addition, self.get("store"))]

        if response.detour is not None:
            detour_wf = flow_to_workflow(response.detour, self.get("store"))
            if detours is not None:
                detours.append(detour_wf)
            else:
                detours = [detour_wf]

        fwa = FWAction(
            stored_data=response.stored_data,
            detours=detours,
            additions=additions,
            defuse_workflow=response.stop_jobflow,
            defuse_children=response.stop_children,
        )
        return fwa
