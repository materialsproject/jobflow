from fireworks.core.firework import Firework, Workflow

from activities.core.activity import Activity


def activity_to_workflow(activity: Activity) -> Workflow:
    # TODO: handle activity config and metadata

    parent_map = {}
    fireworks = []
    for i, (activity, parents) in activity.iteractivity():
        if (
            activity.contains_activities
            and len(parents) == 0
            and activity.outputs is None
        ):
            # activity is a container only so we don't need to do anything
            pass

        # todo handle activities with no parents and no outputs but containing tasks

        activity_firetask = get_activity_firetask(activity)
        parents = [parent_map[parent] for parent in parents]
        fw = Firework(tasks=[activity_firetask], parents=parents, name=activity.name)

        fireworks.append(fw)
        parent_map[activity.uuid] = fw

    return Workflow(fireworks, name=activity.name)
