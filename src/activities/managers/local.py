"""Tools for running activities locally."""
import logging

from activities.core.activity import Activity

logger = logging.getLogger(__name__)


def run_activity_locally(activity: Activity):
    output_cache = {}
    store = {}
    stopped_parents = set()

    def _run(root_activity):
        subactivity: Activity
        for subactivity, parents in root_activity.iteractivity():
            if len(set(parents).intersection(stopped_parents)) > 0:
                # stop children has been called for one of the activities' parents
                logger.info(
                    f"{subactivity.name} is a child of an activity with "
                    f"stop_children=True, skipping..."
                )
                stopped_parents.add(subactivity.uuid)
                continue

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
                continue

            response = subactivity.run(output_cache=output_cache)

            if response.store is not None:
                store[subactivity.uuid] = response.store

            if response.stop_children:
                stopped_parents.add(subactivity.uuid)

            if response.stop_activities:
                break

            if response.detour is not None:
                detour_activity, remaining_tasks = response.detour
                _run(detour_activity)
                _run(remaining_tasks)

            if response.restart is not None:
                pass

    logger.info(f"Started executing activities locally")
    _run(activity)
    logger.info(f"Finished executing activities locally")
    return output_cache
