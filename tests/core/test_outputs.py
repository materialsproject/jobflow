import pytest

from jobflow import job


@pytest.fixture
def add_job():
    @job
    def add(a, b):
        return a + b

    return add


def test_retrieves_jobs_in_single_flow(memory_jobstore, add_job):
    from jobflow import Flow
    from jobflow.core.outputs import OutputManager
    from jobflow.managers.local import run_locally

    j1 = add_job(1, 2)
    j2 = add_job(2, 3)

    flow = Flow([j1, j2])

    run_locally(flow, store=memory_jobstore)

    manager = OutputManager(memory_jobstore)

    all_output_docs = manager.get_all_jobs_in_flow(j1.uuid)
    all_uuids = {d["uuid"] for d in all_output_docs}
    assert len(all_output_docs) == 2

    assert j1.uuid in all_uuids
    assert j2.uuid in all_uuids


def test_retrieves_jobs_in_nested_flows(memory_jobstore, add_job):
    from jobflow import Flow
    from jobflow.core.outputs import OutputManager
    from jobflow.managers.local import run_locally

    j1 = add_job(1, 2)
    j3 = add_job(3, 6)
    j4 = add_job(4, j3.output)
    subflow = Flow([j3, j4], output=j4.output)

    j2 = add_job(2, subflow.output)

    flow = Flow([j1, subflow, j2], output=j2.output)
    run_locally(flow, store=memory_jobstore)

    all_uuids = {j1.uuid, j2.uuid, j3.uuid, j4.uuid}

    mgr = OutputManager(memory_jobstore)

    # Assert returned jobs are the same regardless
    # of where you start

    from_j1 = mgr.get_all_jobs_in_flow(j1.uuid)
    j1_uuids = {p["uuid"] for p in from_j1}
    assert j1_uuids == all_uuids

    from_j2 = mgr.get_all_jobs_in_flow(j2.uuid)
    j2_uuids = {p["uuid"] for p in from_j2}
    assert j2_uuids == all_uuids

    from_j3 = mgr.get_all_jobs_in_flow(j3.uuid)
    j3_uuids = {p["uuid"] for p in from_j3}
    assert j3_uuids == all_uuids

    from_j4 = mgr.get_all_jobs_in_flow(j4.uuid)
    j4_uuids = {p["uuid"] for p in from_j4}
    assert j4_uuids == all_uuids


def test_retrieves_job_parents(memory_jobstore, add_job):
    from jobflow import Flow
    from jobflow.core.outputs import OutputManager
    from jobflow.managers.local import run_locally

    j1 = add_job(1, 2)
    j2 = add_job(2, j1.output)
    j3 = add_job(j1.output, j2.output)

    flow = Flow([j1, j2, j3])
    run_locally(flow, store=memory_jobstore)

    mgr = OutputManager(memory_jobstore)
    j1_parents = mgr.get_job_parents(j1.uuid)
    assert len(j1_parents) == 0

    j2_parents = mgr.get_job_parents(j2.uuid)
    assert len(j2_parents) == 1
    assert j2_parents[0]["uuid"] == j1.uuid

    j3_parents = mgr.get_job_parents(j3.uuid)
    assert len(j3_parents) == 2
    j3_parent_uuids = {p["uuid"] for p in j3_parents}
    assert j1.uuid in j3_parent_uuids
    assert j2.uuid in j3_parent_uuids


def test_retrieves_job_parents_nested_flows(memory_jobstore, add_job):
    from jobflow import Flow
    from jobflow.core.outputs import OutputManager
    from jobflow.managers.local import run_locally

    j1 = add_job(1, 2)
    j3 = add_job(3, 6)
    j4 = add_job(4, j3.output)
    subflow = Flow([j3, j4], output=j4.output)

    j2 = add_job(2, subflow.output)

    flow = Flow([j1, subflow, j2], output=j2.output)
    run_locally(flow, store=memory_jobstore)

    mgr = OutputManager(memory_jobstore)
    j2_parents = mgr.get_job_parents(j2.uuid)
    j2_parent_uuids = [p["uuid"] for p in j2_parents]
    assert len(j2_parent_uuids) == 1
    assert j2_parent_uuids[0] == j4.uuid
