from fireworks.core.firework import Firework, Workflow
from maggma.core import Store

from activities.core.activity import Activity
from activities.managers.fireworks.firetask import ActivityFiretask


def activity_to_workflow(
    activity: Activity,
    output_store: Store
) -> Workflow:
    # TODO: handle activity config and metadata

    parent_map = {}
    fireworks = []
    for activity, parents in activity.iteractivity():
        if (
            activity.contains_activities
            and len(parents) == 0
            and activity.outputs is None
        ):
            # activity is a container only so we don't need to do anything
            # as the activity has no outputs this implies that it will have no children
            continue

        # todo handle activities with no parents and no outputs but containing tasks

        activity_firetask = ActivityFiretask(
            activity=activity, output_store=output_store
        )

        parents = [parent_map[parent] for parent in parents] if parents else None
        fw = Firework(tasks=[activity_firetask], parents=parents, name=activity.name)

        fireworks.append(fw)
        parent_map[activity.uuid] = fw

    return Workflow(fireworks, name=activity.name)
