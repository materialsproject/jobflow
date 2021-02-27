from fireworks import FiretaskBase

from activities.core.activity import Activity


class ActivityFiretask(FiretaskBase):
    required_params = ["activity"]

    def run_task(self, fw_spec):
        activity: Activity = self.get("activity")
        outputs = activity.run()

        if outputs.restart is not None:
            pass

        if outputs.detour is not None:
            pass

        if outputs.store is not None:
            pass
