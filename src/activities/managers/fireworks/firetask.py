from fireworks.core.firework import FiretaskBase, FWAction
from fireworks.utilities.fw_utilities import explicit_serialize


@explicit_serialize
class ActivityFiretask(FiretaskBase):

    required_params = ["activity", "output_store"]

    def run_task(self, fw_spec):
        from activities.core.activity import Activity
        from activities.core.util import initialize_logger
        from activities.managers.fireworks.workflow import activity_to_workflow

        activity: Activity = self.get("activity")
        output_store = self.get("output_store")
        output_store.connect()

        initialize_logger()
        response = activity.run(output_store=output_store)

        detours = None
        if response.detour is not None:
            # create a workflow from the new additions
            detour_wf = activity_to_workflow(response.detour[0], output_store)

            # create a workflow to finish the remaining tasks of the current firework
            complete_task_wf = activity_to_workflow(
                response.detour[1], output_store, iteractivity=False
            )

            # join the workflows together
            detour_wf.append_wf(complete_task_wf, fw_ids=detour_wf.leaf_fw_ids)
            detours = [detour_wf]

        if response.restart is not None:
            pass

        return FWAction(
            stored_data=response.store,
            detours=detours,
            defuse_workflow=response.stop_activities,
            defuse_children=response.stop_children,
        )
