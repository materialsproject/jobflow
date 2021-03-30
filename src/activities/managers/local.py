"""Tools for running activities locally."""
from __future__ import annotations

import logging
import typing

if typing.TYPE_CHECKING:
    import activities

logger = logging.getLogger(__name__)


def run_activity_locally(activity: activities.Activity, log: bool = True):
    from maggma.stores import MemoryStore

    from activities.core.util import initialize_logger

    if log:
        initialize_logger()

    store = MemoryStore()
    store.connect()
    stopped_parents = set()
    responses = {}

    def _run_job(job: activities.Job, parents):
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
        job: activities.Job
        response = None
        for job, parents in root_activity.iteractivity():
            response = _run_job(job, parents)
            if response is False:
                return
            responses[job.uuid] = response
        return response

    logger.info(f"Started executing activities locally")
    _run_iter(activity)
    logger.info(f"Finished executing activities locally")
    return responses
