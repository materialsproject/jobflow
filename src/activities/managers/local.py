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
    stop_activities = False

    def _run_job(job: activities.Job, parents):
        nonlocal stop_activities

        if stop_activities:
            return False

        if len(set(parents).intersection(stopped_parents)) > 0:
            # stop children has been called for one of the jobs' parents
            logger.info(
                f"{job.name} is a child of a job with stop_children=True, skipping..."
            )
            stopped_parents.add(job.uuid)
            return

        response = job.run(store=store)
        responses[job.uuid] = response

        if response.stored_data is not None:
            logger.warning("Response.stored_data is not supported with local manager.")

        if response.stop_children:
            stopped_parents.add(job.uuid)

        if response.stop_activities:
            stop_activities = True
            return False

        if response.restart is not None:
            # first run any restarts
            _run(response.restart)

        if response.detour is not None:
            # next any detours
            _run(response.detour)

        if response.addition is not None:
            # finally any additions
            _run(response.addition)

        return response

    def _run(root_activity):
        if isinstance(root_activity, Job):
            response = _run_job(root_activity, [])
            if response is False:
                return False

        else:
            job: activities.Job
            for job, parents in root_activity.iteractivity():
                response = _run_job(job, parents)
                if response is False:
                    return False

    logger.info(f"Started executing activities locally")
    _run(activity)
    logger.info(f"Finished executing activities locally")
    return responses
