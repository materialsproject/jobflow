"""Define classes related to accessing job and Flow outputs."""
from __future__ import annotations

from jobflow.schemas.job_store import JobStoreDocument
from jobflow.settings import JobflowSettings

from .store import JobStore


def get_flow_tree_from_host_lists(host_lists) -> dict:
    """
    Construct a tree representing flow nesting from by a set of host lists.

    Parameters
    ----------
    host_lists
        A list of lists of strings retrieved from the .hosts attribute on
        JobStoreDocuments.

    Returns
    -------
    Dict
        A dictionary representing the flow nesting.
    """
    flow_parents: dict[str, dict] = {}
    for hl in host_lists:
        curr_flow_parents = flow_parents
        for flow_uuid in reversed(hl):

            if flow_uuid not in curr_flow_parents:
                curr_flow_parents[flow_uuid] = {}

            curr_flow_parents = curr_flow_parents[flow_uuid]
    return flow_parents


def get_flow_output_from_tree(
    flow_uuid, flow_tree, parent_flow=None, job_parent_map=None
):
    """
    Construct a FlowOutput from a flow tree.

    Parameters
    ----------
    flow_uuid
        The UUID of the flow containing the flow tree.

    flow_tree
        The flow tree from which flow outputs should be recursively
        reconstructed.

    parent_flow
        A reference to the flow containing this flow, if any.

    job_parent_map
        A dict mapping flow UUIDs to list of JobStoreDocuments which
        are contained by those flows.

    Returns
    -------
    Dict
        A dictionary representing the flow nesting.
    """
    if len(flow_tree) == 0:
        output = FlowOutput(flow_uuid, parent_flow=parent_flow)
    else:
        output = FlowOutput(flow_uuid, parent_flow=parent_flow)
        for flow_uuid, sub_flow_tree in flow_tree.items():
            sub_flow_output = get_flow_output_from_tree(
                flow_uuid,
                sub_flow_tree,
                parent_flow=output,
                job_parent_map=job_parent_map,
            )
            output.add_output(sub_flow_output)

    if job_parent_map is not None:
        for job_doc in job_parent_map.get(flow_uuid):
            output.add_output(job_doc)

    return output


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

    def __init__(self, store: JobStore = None):
        if store is None:
            store = JobflowSettings().JOB_STORE

        self._store = store

    def construct_flow_from_job(self, job_uuid: str):
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
        if job_doc is None:
            raise ValueError(f"No jobs found in store with uuid {job_uuid}")

        parent_flow_id = job_doc["hosts"][-1]
        all_job_dicts = list(self._store.query({"hosts": parent_flow_id}))
        all_job_docs = [JobStoreDocument(**d) for d in all_job_dicts]

        parent_job_flows: dict[str, list[JobStoreDocument]] = {}

        for job_doc in all_job_docs:
            parent_flow_uuid = job_doc.hosts[0]
            if parent_flow_uuid not in parent_job_flows:
                parent_job_flows[parent_flow_uuid] = []

            parent_job_flows[parent_flow_uuid].append(job_doc)

        host_lists = [d.hosts for d in all_job_docs]
        flow_tree = get_flow_tree_from_host_lists(host_lists)

        top_flow_uuid = next(iter(flow_tree.keys()))
        return get_flow_output_from_tree(
            top_flow_uuid, flow_tree[top_flow_uuid], job_parent_map=parent_job_flows
        )

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
        raw_docs = list(self._store.query({"uuid": {"$in": parent_uuids}}))
        return [JobStoreDocument(**d) for d in raw_docs]


class FlowOutput:
    """
    A :obj:`FlowOutput` provides methods for retrieving outputs of jobs in a flow.

    It retains information about the connectedness of jobs and allows the user
    to retrieve job outputs by navigating the flow graph.

    Parameters
    ----------
    store
        The JobStore used for retrieving outputs.

    Returns
    -------
    FlowOutput
        An FlowOutput instance.

    See Also
    --------
    JobStore
    """

    def __init__(
        self,
        flow_uuid: str,
        containing_flow_output: FlowOutput = None,
        store: JobStore = None,
    ):
        if store is None:
            store = JobflowSettings().JOB_STORE

        self._store = store
        self.uuid = flow_uuid
        self.containing_flow = containing_flow_output
        self._job_outputs: list[JobStoreDocument] = []
        self._flow_outputs: list[FlowOutput] = []

    def add_output(self, output: FlowOutput | JobStoreDocument) -> None:
        """
        Add an output to the record of outputs contained in this flow.

        Output can be either a FlowOutput or a JobStoreDocument.

        Returns
        -------
        None

        """
        if type(output) == FlowOutput:
            self._flow_outputs.append(output)
        elif type(output) == JobStoreDocument:
            self._job_outputs.append(output)

    def immediate_job_outputs(self) -> list[JobStoreDocument]:
        """
        Retrieve the outputs of the jobs which are immediate children of this flow.

        Returns
        -------
        List[JobStoreDocument]
            A list of output documents.
        """
        return self._job_outputs

    def all_job_outputs(self) -> list[JobStoreDocument]:
        """
        Recursively retrieve job outputs in this flow.

        Returns
        -------
        List[JobStoreDocument]
            A list of output documents.
        """
        outputs = self.immediate_job_outputs()
        for flow_output in self.flow_outputs():
            subflow_outputs = flow_output.all_job_outputs()
            outputs = [*outputs, *subflow_outputs]
        return outputs

    def flow_outputs(self) -> list[FlowOutput]:
        """
        Retrieve the outputs of flows inside this flow.

        Returns
        -------
        List[FlowOutput]
            A list of FlowOutput objects.
        """
        return self._flow_outputs

    def get_job_prerequisites(
        self, job_doc: JobStoreDocument
    ) -> list[JobStoreDocument]:
        """
        Retrieve the outputs of the jobs referenced as inputs in the provided job.

        Parameters
        ----------
        job_doc
            The job document whose parents should be retrieved.

        Returns
        -------
        List[JobStoreDocument]
            A list of job output documents.
        """
        prereq_uuids = [ref.uuid for ref in job_doc.input_references]
        inputs = [self.get_job_document(u) for u in prereq_uuids]
        return inputs

    def containing_flow_list(self) -> list[FlowOutput]:
        """
        Retrieve the list of flows in which this flow is nested.

        Similar to the hosts property of a JobStoreDocument.

        Returns
        -------
        List[dict]
            A list of output documents.
        """
        return [self.containing_flow, *self.containing_flow.containing_flow_list()]

    def get_job_document(self, job_uuid) -> JobStoreDocument | None:
        """
        Retrieve the output document for a job in this flow or one of its children.

        Parameters
        ----------
        job_doc
            The job document which should be retrieved.

        Returns
        -------
        List[dict]
            A list of output documents.
        """
        filtered = [d for d in self._job_outputs if d.uuid == job_uuid]
        if len(filtered) > 0:
            return filtered[0]
        else:
            for flow in self.flow_outputs():
                retrieved = flow.get_job_document(job_uuid)
                if retrieved is not None:
                    return retrieved

            return None
