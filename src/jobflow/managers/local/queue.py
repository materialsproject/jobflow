"""Define base Queue class for managing running jobs and flows."""

from __future__ import annotations

import logging
import typing

from jobflow.utils import suuid

if typing.TYPE_CHECKING:
    from typing import List, Optional, Union

    from maggma.core import Store

    import jobflow

__all__ = ["Queue"]


logger = logging.getLogger(__name__)


class Queue:
    def __init__(self, queue_store: Store = None):
        from jobflow import SETTINGS

        if queue_store is None:
            queue_store = SETTINGS.QUEUE_STORE

        self.queue_store = queue_store
        self.queue_store.connect()

    def get_flow_info_by_flow_uuid(self, flow_uuid, properties=None):
        return self.queue_store.query_one(
            {"type": "flow", "uuid": flow_uuid}, properties
        )

    def get_flow_info_by_job_uuid(self, job_uuid, properties=None):
        return self.queue_store.query_one(
            {"type": "flow", "jobs": job_uuid}, properties
        )

    def get_job_info_by_job_uuid(self, job_uuid, job_index, properties=None):
        query = {"type": "job", "uuid": job_uuid, "index": job_index}
        if job_index is not None:
            query["index"] = job_index
        return self.queue_store.query_one(query, properties)

    def get_jobs_info_by_job_uuids(self, job_uuids, properties=None):
        query = {"type": "job", "uuid": {"$in": job_uuids}}
        return self.queue_store.query(query, properties)

    def get_jobs_info_by_flow_uuid(self, flow_uuid, properties=None):
        job_uuids = self.get_flow_info_by_flow_uuid(flow_uuid, ["jobs"])["jobs"]
        return self.get_jobs_info_by_job_uuids(job_uuids, properties)

    def add_flow(self, flow: Union[jobflow.Flow, jobflow.Job, List[jobflow.Job]]):
        from jobflow.core.flow import get_flow

        flow = get_flow(flow)

        flow_dict = {
            "type": "flow",
            "uuid": flow.uuid,
            "jobs": flow.job_uuids,
        }

        job_dicts = []
        for job, parents in flow.iterflow():
            job_dicts.append(_get_job_dict(job, parents))

        self.queue_store.update(flow_dict, key="uuid")
        self.queue_store.update(job_dicts, key=["uuid", "index"])

        logger.info(f"Added flow ({flow.uuid}) with jobs: {flow.job_uuids}")

    def append_flow(
        self,
        job_uuid: str,
        job_index: int,
        new_flow: Union[jobflow.Flow, jobflow.Job, List[jobflow.Job]],
        detour: bool = False,
    ):
        from jobflow.core.flow import get_flow

        new_flow = get_flow(new_flow)

        # add new jobs to flow
        flow_dict = self.get_flow_info_by_job_uuid(job_uuid)
        flow_dict["jobs"].extend(new_flow.job_uuids)

        # get job parents
        result = self.get_job_info_by_job_uuid(job_uuid, job_index, ["parents"])
        job_parents = result["parents"]

        # add new jobs
        job_dicts = []
        for job, parents in new_flow.iterflow():
            # inherit the parents of the job to which we are appending
            parents = parents if parents else job_parents
            job_dicts.append(_get_job_dict(job, parents))

        if detour:
            # if detour, then update the parents of the child jobs
            leaf_uuids = [v for v, d in new_flow.graph.out_degree() if d == 0]
            children = list(
                self.queue_store.query({"type": "job", "parents": job_uuid})
            )
            for child in children:
                child["parents"].extend(leaf_uuids)
            self.queue_store.update(children, key=["uuid", "index"])

        self.queue_store.update(flow_dict, key="uuid")
        self.queue_store.update(job_dicts, key=["uuid", "index"])

        logger.info(f"Appended flow ({new_flow.uuid}) with jobs: {new_flow.job_uuids}")

    def checkout_job(
        self, query=None, launch_dir=None, flow_uuid: str = None
    ) -> Optional[jobflow.Job]:
        from jobflow import Job

        query = {} if query is None else dict(query)
        query.update({"state": "ready", "type": "job"})

        if flow_uuid is not None:
            # if flow uuid provided, only include job ids in that flow
            job_uuids = self.get_flow_info_by_flow_uuid(flow_uuid, ["jobs"])["jobs"]
            query["uuid"] = {"$in": job_uuids}

        reserve_id = suuid()
        result = self.queue_store._collection.update_one(
            query,
            {"$set": {"state": "running", "reserve_id": reserve_id}},
            upsert=False,
        )

        if result.modified_count < 1:
            return None

        result = self.queue_store.query_one(
            {"reserve_id": reserve_id}, properties=["job"]
        )
        return Job.from_dict(result["job"])

    def reserve_job(self, job_dict: dict, launch_dir: str | None):
        if job_dict["state"] == "running":
            return False

        reserve_uuid = suuid()

        # Upload the full doc: set job state to running and add launch dir
        job_dict.update(
            {"reserve_uuid": reserve_uuid, "state": "running", "launch_dir": launch_dir}
        )

        self.queue_store.update(job_dict, key=["uuid", "index"])

        # maggma stores don't implement find one and update, so have to update the entire
        # document at once. This can be quite slow and mean the same job can be checked
        # out twice. Therefore do a quick query after updating to check no other job has
        # checked the job out
        job_dict = self.queue_store.query_one(
            {"uuid": job_dict["uuid"], "index": job_dict["index"]}
        )

        # if we get a different reserve uuid then the job was stolen by another process
        return job_dict.get("reserve_uuid", None) == reserve_uuid

    def checkin_job(self, job: jobflow.Job, response: Optional[jobflow.Response]):
        job_dict = self.get_job_info_by_job_uuid(job.uuid, job.index)
        job_dict["state"] = "failed" if response is None else "completed"
        self.queue_store.update(job_dict, key=["uuid", "index"])

        # handle response
        if response is not None:
            if response.replace is not None:
                self.append_flow(job.uuid, job.index, response.replace)

            if response.addition is not None:
                self.append_flow(job.uuid, job.index, response.addition)

            if response.detour is not None:
                self.append_flow(job.uuid, job.index, response.detour, detour=True)

            if response.stored_data is not None:
                logger.warning(
                    "Response.stored_data is not supported with local manager"
                )

            if response.stop_children:
                self.stop_children(job.uuid)

            if response.stop_jobflow:
                self.stop_jobflow(job_uuid=job.uuid)

        job_uuids = self.get_flow_info_by_job_uuid(job.uuid, ["jobs"])["jobs"]
        self.refresh_states(job_uuids)

    def refresh_states(self, job_uuids):
        # go through and look for jobs whose state we can update to ready
        # need to ensure that all parent uuids with all indices are completed
        # first find state of all jobs; ensure larger indices are returned last
        results = self.queue_store.query(
            {"type": "job", "uuid": {"$in": job_uuids}}, sort={"index": 1}
        )
        mapping = {r["uuid"]: r for r in results}

        # now find jobs that are queued and whose parents are all completed and ready them
        updates = []
        for uuid, job in mapping.items():
            if job["state"] == "queued" and all(
                [mapping[p]["state"] == "completed" for p in job["parents"]]
            ):
                job["state"] = "ready"
                updates.append(job)

        if len(updates) > 0:
            self.queue_store.update(updates, key=["index", "uuid"])

    def stop_children(self, job_uuid: str):
        children = self.queue_store.query(
            {"type": "job", "parents": job_uuid, "state": "queued"}
        )
        children = list(children)
        for child in children:
            child["state"] = "stopped"
        self.queue_store.update(children, key=["uuid", "index"])

    def stop_jobflow(self, job_uuid: str = None, flow_uuid: str = None):
        if job_uuid is None and flow_uuid is None:
            raise ValueError("Either job_uuid or flow_uuid must be set.")

        if job_uuid is not None and flow_uuid is not None:
            raise ValueError("Only one of job_uuid and flow_uuid should be set.")

        if job_uuid is not None:
            criteria = {"type": "flow", "jobs": job_uuid}
        else:
            criteria = {"type": "flow", "uuid": flow_uuid}

        # get uuids of jobs in the flow
        job_uuids = self.queue_store.query_one(criteria, ["jobs"])["jobs"]

        # set the state of the jobs to stopped
        jobs = self.queue_store.query(
            {"type": "job", "uuid": {"$in": job_uuids}, "state": "queued"},
        )
        jobs = list(jobs)
        for job in jobs:
            job["state"] = "stopped"
        self.queue_store.update(jobs, key=["uuid", "index"])


def _get_job_dict(job, parents):
    from monty.json import jsanitize

    return {
        "job": jsanitize(job, strict=True, enum_values=True),
        "uuid": job.uuid,
        "index": job.index,
        "type": "job",
        "parents": parents,
        "state": "queued" if parents else "ready",
        "launch_dir": None,
    }
