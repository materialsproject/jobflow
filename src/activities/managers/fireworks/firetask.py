from fireworks.core.firework import FiretaskBase
from fireworks.utilities.fw_utilities import explicit_serialize


@explicit_serialize
class ActivityFiretask(FiretaskBase):

    required_params = ["activity", "output_store"]

    def run_task(self, fw_spec):
        from activities.core.activity import Activity
        from activities.core.util import initialize_logger

        activity: Activity = self.get("activity")
        output_store = self.get("output_store")
        output_store.connect()

        initialize_logger()
        outputs = activity.run(output_store=output_store)

        if outputs.restart is not None:
            pass

        if outputs.detour is not None:
            pass

        if outputs.store is not None:
            pass
