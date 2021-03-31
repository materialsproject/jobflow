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

        initialize_logger()
        response = job.run(store=store)

        detours = None
        if response.restart is not None:
            # create a workflow from the new additions
            detours = [activity_to_workflow(response.restart, store)]

        if response.restart is not None:
            pass

        return FWAction(
            stored_data=response.store,
            detours=detours,
            defuse_workflow=response.stop_activities,
            defuse_children=response.stop_children,
        )
