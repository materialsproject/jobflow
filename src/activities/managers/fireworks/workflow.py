from fireworks.core.firework import Firework, Workflow
from maggma.core import Store

from activities.core.activity import Activity
from activities.managers.fireworks.firetask import JobFiretask


def activity_to_workflow(activity: Activity, store: Store) -> Workflow:
    # TODO: handle activity config and metadata

    parent_map = {}
    fireworks = []

    for job, parents in activity.iteractivity():
        job_firetask = JobFiretask(job=job, store=store)
        job_parents = [parent_map[parent] for parent in parents] if parents else None
        fw = Firework(tasks=[job_firetask], parents=job_parents, name=job.name)
        parent_map[job.uuid] = fw

        fireworks.append(fw)

    return Workflow(fireworks, name=activity.name)
