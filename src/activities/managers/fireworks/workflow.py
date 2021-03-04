from fireworks.core.firework import Firework, Workflow
from maggma.core import Store

from activities.core.activity import Activity
from activities.managers.fireworks.firetask import ActivityFiretask


def activity_to_workflow(
    activity: Activity,
    output_store: Store,
    iteractivity: bool = True
) -> Workflow:
    # TODO: handle activity config and metadata

    parent_map = {}
    fireworks = []

    def _to_fw(_activity, _parents):
        # todo handle activities with no parents and no outputs but containing tasks

        activity_firetask = ActivityFiretask(
            activity=_activity, output_store=output_store
        )

        _parents = [parent_map[parent] for parent in _parents] if _parents else None
        fw = Firework(tasks=[activity_firetask], parents=_parents, name=_activity.name)
        parent_map[_activity.uuid] = fw
        return fw

    if iteractivity:
        for subactivity, parents in activity.iteractivity():
            if (
                subactivity.contains_activities
                and len(parents) == 0
                and subactivity.outputs is None
            ):
                # activity is a container only so we don't need to do anything as the
                # activity has no outputs this implies that it will have no children
                continue
            fireworks.append(_to_fw(subactivity, parents))
    else:
        fireworks.append(_to_fw(activity, None))

    return Workflow(fireworks, name=activity.name)
