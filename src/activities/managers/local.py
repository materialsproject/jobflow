"""Tools for running activities locally."""
import logging

from maggma.stores import MemoryStore

from activities import Job
from activities.core.activity import Activity

logger = logging.getLogger(__name__)


def run_activity_locally(activity: Activity):
    output_cache = {}
    store = MemoryStore()
    store.connect()
    stopped_parents = set()

    def _run_job(job: Job, parents):
        if len(set(parents).intersection(stopped_parents)) > 0:
            # stop children has been called for one of the jobs' parents
            logger.info(
                f"{job.name} is a child of a job with "
                f"stop_children=True, skipping..."
            )
            stopped_parents.add(job.uuid)
            return True

        response = job.run(store=store)

        if response.stop_children:
            stopped_parents.add(job.uuid)

        if response.stop_activities:
            return False

        if response.detour is not None:
            return _run_iter(response.detour)

        if response.restart is not None:
            pass

        return response

    def _run_iter(root_activity):
        job: Job
        response = None
        for job, parents in root_activity.iteractivity():
            response = _run_job(job, parents)
            if response is False:
                return
        return response

    logger.info(f"Started executing activities locally")
    r = _run_iter(activity)
    logger.info(f"Finished executing activities locally")
    return r
