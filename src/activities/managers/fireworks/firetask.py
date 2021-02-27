from fireworks import FiretaskBase

from activities.core.activity import Activity


class ActivityFiretask(FiretaskBase):

    required_params = ["activity", "output_store"]

    def run_task(self, fw_spec):
        activity: Activity = self.get("activity")
        output_store = self.get("output_store")

        outputs = activity.run(output_store=output_store)

        if outputs.restart is not None:
            pass

        if outputs.detour is not None:
            pass

        if outputs.store is not None:
            pass
