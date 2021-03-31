from fireworks.core.firework import FiretaskBase, FWAction
from fireworks.utilities.fw_utilities import explicit_serialize


@explicit_serialize
class JobFiretask(FiretaskBase):

    required_params = ["job", "store"]

    def run_task(self, fw_spec):
        from activities.core.job import Job
        from activities.core.util import initialize_logger
        from activities.managers.fireworks.workflow import activity_to_workflow

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
