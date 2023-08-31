"""Define classes related to accessing job and Flow outputs."""

from .store import JobStore


class OutputManager:
    """
    An :obj:`OutputManager` provides functions for retrieving job outputs.

    It is primarily concerned with identifying the relationships between
    jobs, and allowing the traversal of the job graph after the job has run.

    Parameters
    ----------
    store
        The JobStore used for retrieving outputs.

    Returns
    -------
    OutputManager
        An OutputManager instance.

    See Also
    --------
    JobStore
    """

    def __init__(self, store: JobStore):
        self._store = store

    def get_all_jobs_in_flow(self, job_uuid: str):
        """
        Retrieve output documents for every job in the flow that contains this job.

        Parameters
        ----------
        job_uuid
            The UUID of the job to start searching from.

        Returns
        -------
        List[JobOutputDoc]
            A list of output documents for the jobs in the containing flow
        """
        job_doc = self._store.query_one({"uuid": job_uuid})
        parent_flow_id = job_doc["hosts"][-1]
        all_job_docs = list(self._store.query({"hosts": parent_flow_id}))
        return all_job_docs

    def get_job_parents(self, job_uuid: str):
        """
        Retrieve the output documents associated with job parents.

        Parameters
        ----------
        job_uuid
            The UUID of the job whose parent outputs should be
            retrieved.

        Returns
        -------
        List[dict]
            A list of output documents for the job's parents.
        """
        job_doc = self._store.query_one({"uuid": job_uuid})
        parent_uuids = [r["uuid"] for r in job_doc["input_references"]]
        return list(self._store.query({"uuid": {"$in": parent_uuids}}))
