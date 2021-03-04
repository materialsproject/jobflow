"""Tools for running activities locally."""
import logging

from activities.core.activity import Activity

logger = logging.getLogger(__name__)


def run_activity_locally(activity: Activity):
    output_cache = {}
    store = {}
    stopped_parents = set()

    def _run_activity(subactivity, parents):
        if len(set(parents).intersection(stopped_parents)) > 0:
            # stop children has been called for one of the activities' parents
            logger.info(
                f"{subactivity.name} is a child of an activity with "
                f"stop_children=True, skipping..."
            )
            stopped_parents.add(subactivity.uuid)
            return True

        if (
            subactivity.contains_activities
            and len(parents) == 0
            and subactivity.outputs is None
        ):
            # subactivity is a container only so we don't need to do anything
            logger.info(
                f"{subactivity.name} activity has no outputs and no tasks, "
                "skipping..."
            )
            return True

        response = subactivity.run(output_cache=output_cache)

        if response.store is not None:
            store[subactivity.uuid] = response.store

        if response.stop_children:
            stopped_parents.add(subactivity.uuid)

        if response.stop_activities:
            return False

        if response.detour is not None:
            detour_activity, remaining_tasks = response.detour
            _run_iter(detour_activity)
            return _run_activity(remaining_tasks, [])

        if response.restart is not None:
            pass

        return True

    def _run_iter(root_activity):
        subactivity: Activity
        for subactivity, parents in root_activity.iteractivity():
            continue_run = _run_activity(subactivity, parents)
            if not continue_run:
                break

    logger.info(f"Started executing activities locally")
    _run_iter(activity)
    logger.info(f"Finished executing activities locally")
    return output_cache
