"""Tools for running activities locally."""
import logging

from activities.core.activity import Activity

logger = logging.getLogger(__name__)


def run_activity_locally(activity: Activity):
    logger.info(f"Started executing activities locally")

    output_cache = {}
    activity: Activity
    for activity, parents in activity.iteractivity():
        if (
            activity.contains_activities
            and len(parents) == 0
            and activity.outputs is None
        ):
            # activity is a container only so we don't need to do anything
            logger.info(
                f"{activity.name} activity has no outputs and no tasks, skipping..."
            )
            continue

        outputs = activity.run(output_cache=output_cache)

        if outputs.restart is not None:
            pass

        if outputs.detour is not None:
            pass

        if outputs.store is not None:
            pass

    logger.info(f"Finished executing activities locally")
    return output_cache
