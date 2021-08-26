"""Utility tools for managers."""

from __future__ import annotations

from typing import List, Union

import jobflow
from jobflow.core.reference import find_and_get_references

__all__ = ["get_flow"]


def get_flow(
    flow: Union[jobflow.Flow, jobflow.Job, List[jobflow.Job]],
) -> jobflow.Flow:
    """
    Check dependencies and return flow object.

    Parameters
    ----------
    flow
        A job, list of jobs, or flow.

    Returns
    -------
    Flow
        A :obj:`Flow` object where connections have been checked.
    """
    if not isinstance(flow, jobflow.Flow):
        flow = jobflow.Flow(jobs=flow)

    # ensure that we have all the jobs needed to resolve the reference connections
    job_references = find_and_get_references(flow.jobs)
    job_reference_uuids = {ref.uuid for ref in job_references}
    missing_jobs = job_reference_uuids.difference(set(flow.job_uuids))
    if len(missing_jobs) > 0:
        raise ValueError(
            "The following jobs were not found in the jobs array and are needed to "
            f"resolve output references:\n{list(missing_jobs)}"
        )

    return flow
