"""Tools for running activities locally."""
from __future__ import annotations

import logging
import typing

if typing.TYPE_CHECKING:
    from typing import List, Optional, Union

    from maggma.stores import Store

    import activities

logger = logging.getLogger(__name__)


def run_locally(
    activity: Union[activities.Activity, activities.Job, List[activities.Job]],
    log: bool = True,
    store: Optional[Store] = None,
):
    from maggma.stores import MemoryStore

    from activities import Activity, Job, initialize_logger

    if store is None:
        store = MemoryStore()
        store.connect()

    if log:
        initialize_logger()

    if not isinstance(activity, Activity):
        activity = Activity(jobs=activity)

    stopped_parents = set()
    responses = {}

    def _run_job(job: activities.Job, parents):
        if len(set(parents).intersection(stopped_parents)) > 0:
            # stop children has been called for one of the jobs' parents
            logger.info(
                f"{job.name} is a child of a job with stop_children=True, skipping..."
            )
            stopped_parents.add(job.uuid)
            return True

        response = job.run(store=store)
        responses[job.uuid] = response

        if response.stop_children:
            stopped_parents.add(job.uuid)

        if response.stop_activities:
            return False

        if response.restart is not None:
            if isinstance(response.restart, Job):
                return _run_job(response.restart, [])
            else:
                return _run_iter(response.restart)

        return response

    def _run_iter(root_activity):
        job: activities.Job
        for job, parents in root_activity.iteractivity():
            response = _run_job(job, parents)
            if response is False:
                return

    logger.info(f"Started executing activities locally")
    _run_iter(activity)
    logger.info(f"Finished executing activities locally")
    return responses
