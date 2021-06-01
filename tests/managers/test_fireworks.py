import pytest


def test_flow_to_workflow(
    memory_jobstore, simple_job, simple_flow, connected_flow, nested_flow
):
    from fireworks import Workflow

    from jobflow import OnMissing
    from jobflow.managers.fireworks import flow_to_workflow

    # test simple job
    flow = simple_job()
    wf = flow_to_workflow(flow, memory_jobstore)

    assert type(wf) == Workflow
    assert wf.name == "Flow"
    assert len(wf.fws) == 1
    assert wf.fws[0].name == "func"

    # test simple flow
    flow = simple_flow()
    wf = flow_to_workflow(flow, memory_jobstore)

    assert type(wf) == Workflow
    assert wf.name == "Flow"
    assert len(wf.fws) == 1
    assert wf.fws[0].name == "func"

    # test connected flow
    flow = connected_flow()
    wf = flow_to_workflow(flow, memory_jobstore)

    assert type(wf) == Workflow
    assert wf.name == "Connected Flow"
    assert len(wf.fws) == 2
    assert wf.fws[0].name == "func"
    assert wf.fws[1].name == "func"

    # test nested flow
    flow = nested_flow()
    wf = flow_to_workflow(flow, memory_jobstore)

    assert len(wf.fws) == 4
    assert wf.fws[0].name == "func"
    assert wf.fws[1].name == "func"
    assert wf.fws[2].name == "func"
    assert wf.fws[3].name == "func"

    # test on missing causes allow fizzled parents
    flow = connected_flow()
    flow.jobs[0].config.on_missing_references = OnMissing.NONE
    wf = flow_to_workflow(flow, memory_jobstore)

    assert wf.fws[0].spec["_allow_fizzled_parents"] is True

    # test manager config
    flow = connected_flow()
    flow.jobs[0].config.manager_config = {"metadata": 5}
    wf = flow_to_workflow(flow, memory_jobstore)
    assert wf.fws[0].spec["metadata"] == 5


def test_job_to_firework(
    memory_jobstore, simple_job, simple_flow, connected_flow, nested_flow
):
    from fireworks import Firework

    from jobflow.managers.fireworks import job_to_firework

    # test simple job
    job = simple_job()
    fw = job_to_firework(job, memory_jobstore)

    assert type(fw) == Firework
    assert fw.name == "func"

    job2 = simple_job()
    fw = job_to_firework(
        job2, memory_jobstore, parents=[job.uuid], parent_mapping={job.uuid: 1}
    )

    assert type(fw) == Firework
    assert fw.name == "func"

    with pytest.raises(ValueError):
        job_to_firework(job2, memory_jobstore, parents=[job.uuid])


def test_simple_flow(lpad, mongo_jobstore, fw_dir, simple_flow, capsys):
    from fireworks.core.rocket_launcher import rapidfire

    from jobflow.managers.fireworks import flow_to_workflow

    flow = simple_flow()
    uuid = flow.jobs[0].uuid

    wf = flow_to_workflow(flow, mongo_jobstore)
    fw_ids = lpad.add_wf(wf)

    # run the workflow
    rapidfire(lpad)

    # check workflow completed
    fw_id = list(fw_ids.values())[0]
    wf = lpad.get_wf_by_fw_id(fw_id)

    assert all([s == "COMPLETED" for s in wf.fw_states.values()])

    # check store has the activity output
    result = mongo_jobstore.query_one({"uuid": uuid})
    assert result["output"] == "12345_end"

    # check logs printed
    captured = capsys.readouterr()
    assert "INFO Starting job - func" in captured.out
    assert "INFO Finished job - func" in captured.out


def test_simple_flow_metadata(lpad, mongo_jobstore, fw_dir, simple_flow, capsys):
    from fireworks.core.rocket_launcher import rapidfire

    from jobflow.managers.fireworks import flow_to_workflow

    flow = simple_flow()
    uuid = flow.jobs[0].uuid

    wf = flow_to_workflow(flow, mongo_jobstore)
    fw_ids = lpad.add_wf(wf)

    # run the workflow
    rapidfire(lpad)

    # check workflow completed
    fw_id = list(fw_ids.values())[0]
    wf = lpad.get_wf_by_fw_id(fw_id)

    assert all([s == "COMPLETED" for s in wf.fw_states.values()])

    # check store has the activity output
    result = mongo_jobstore.query_one({"uuid": uuid})
    assert result["output"] == "12345_end"
    assert result["metadata"]["fw_id"] == fw_id

    # test override
    flow = simple_flow()
    flow.jobs[0].config.manager_config = {"_add_launchpad_and_fw_id": False}
    uuid = flow.jobs[0].uuid

    wf = flow_to_workflow(flow, mongo_jobstore)
    lpad.add_wf(wf)

    # run the workflow
    rapidfire(lpad)

    result = mongo_jobstore.query_one({"uuid": uuid})
    assert result["metadata"] == {}


def test_connected_flow(lpad, mongo_jobstore, fw_dir, connected_flow, capsys):
    from fireworks.core.rocket_launcher import rapidfire

    from jobflow.managers.fireworks import flow_to_workflow

    flow = connected_flow()
    uuid1 = flow.jobs[0].uuid
    uuid2 = flow.jobs[1].uuid

    wf = flow_to_workflow(flow, mongo_jobstore)
    fw_ids = lpad.add_wf(wf)

    # run the workflow
    rapidfire(lpad)

    # check workflow completed
    fw_id = list(fw_ids.values())[0]
    wf = lpad.get_wf_by_fw_id(fw_id)

    assert all([s == "COMPLETED" for s in wf.fw_states.values()])

    # check store has the activity output
    result1 = mongo_jobstore.query_one({"uuid": uuid1})
    result2 = mongo_jobstore.query_one({"uuid": uuid2})

    assert result1["output"] == "12345_end"
    assert result2["output"] == "12345_end_end"


def test_nested_flow(lpad, mongo_jobstore, fw_dir, nested_flow, capsys):
    from fireworks.core.rocket_launcher import rapidfire

    from jobflow.managers.fireworks import flow_to_workflow

    flow = nested_flow()
    uuid1 = flow.jobs[0].jobs[0].uuid
    uuid2 = flow.jobs[0].jobs[1].uuid
    uuid3 = flow.jobs[1].jobs[0].uuid
    uuid4 = flow.jobs[1].jobs[1].uuid

    wf = flow_to_workflow(flow, mongo_jobstore)
    fw_ids = lpad.add_wf(wf)

    # run the workflow
    rapidfire(lpad)

    # check workflow completed
    fw_id = list(fw_ids.values())[0]
    wf = lpad.get_wf_by_fw_id(fw_id)

    assert all([s == "COMPLETED" for s in wf.fw_states.values()])

    # check store has the activity output
    result1 = mongo_jobstore.query_one({"uuid": uuid1})
    result2 = mongo_jobstore.query_one({"uuid": uuid2})
    result3 = mongo_jobstore.query_one({"uuid": uuid3})
    result4 = mongo_jobstore.query_one({"uuid": uuid4})

    assert result1["output"] == "12345_end"
    assert result2["output"] == "12345_end_end"
    assert result3["output"] == "12345_end_end_end"
    assert result4["output"] == "12345_end_end_end_end"


def test_addition_flow(lpad, mongo_jobstore, fw_dir, addition_flow, capsys):
    from fireworks.core.rocket_launcher import rapidfire

    from jobflow.managers.fireworks import flow_to_workflow

    flow = addition_flow()
    uuid1 = flow.jobs[0].uuid

    wf = flow_to_workflow(flow, mongo_jobstore)
    fw_ids = lpad.add_wf(wf)

    # run the workflow
    rapidfire(lpad)

    # check workflow completed
    fw_id = list(fw_ids.values())[0]
    wf = lpad.get_wf_by_fw_id(fw_id)

    uuids = [fw.tasks[0]["job"].uuid for fw in wf.fws]
    uuid2 = [u for u in uuids if u != uuid1][0]
    assert all([s == "COMPLETED" for s in wf.fw_states.values()])

    # check store has the activity output
    result1 = mongo_jobstore.query_one({"uuid": uuid1})
    result2 = mongo_jobstore.query_one({"uuid": uuid2})

    assert result1["output"] == 11
    assert result2["output"] == "11_end"


def test_detour_flow(lpad, mongo_jobstore, fw_dir, detour_flow, capsys):
    from fireworks.core.rocket_launcher import rapidfire

    from jobflow.managers.fireworks import flow_to_workflow

    flow = detour_flow()
    uuid1 = flow.jobs[0].uuid
    uuid3 = flow.jobs[1].uuid

    wf = flow_to_workflow(flow, mongo_jobstore)
    fw_ids = lpad.add_wf(wf)

    # run the workflow
    rapidfire(lpad)

    # check workflow completed
    fw_id = list(fw_ids.values())[0]
    wf = lpad.get_wf_by_fw_id(fw_id)

    uuids = [fw.tasks[0]["job"].uuid for fw in wf.fws]
    uuid2 = [u for u in uuids if u != uuid1 and u != uuid3][0]
    assert all([s == "COMPLETED" for s in wf.fw_states.values()])

    # check store has the activity output
    result1 = mongo_jobstore.query_one({"uuid": uuid1})
    result2 = mongo_jobstore.query_one({"uuid": uuid2})
    result3 = mongo_jobstore.query_one({"uuid": uuid3})

    assert result1["output"] == 11
    assert result2["output"] == "11_end"
    assert result3["output"] == "12345_end"

    # assert job2 (detoured job) ran before job3
    assert result2["completed_at"] < result3["completed_at"]


def test_replace_flow(lpad, mongo_jobstore, fw_dir, replace_flow, capsys):
    from fireworks.core.rocket_launcher import rapidfire

    from jobflow.managers.fireworks import flow_to_workflow

    flow = replace_flow()
    uuid1 = flow.jobs[0].uuid
    uuid2 = flow.jobs[1].uuid

    wf = flow_to_workflow(flow, mongo_jobstore)
    fw_ids = lpad.add_wf(wf)

    # run the workflow
    rapidfire(lpad)

    # check workflow completed
    fw_id = list(fw_ids.values())[0]
    wf = lpad.get_wf_by_fw_id(fw_id)

    assert all([s == "COMPLETED" for s in wf.fw_states.values()])

    # check store has the activity output
    result1 = mongo_jobstore.query_one({"uuid": uuid1, "index": 1})
    result2 = mongo_jobstore.query_one({"uuid": uuid1, "index": 2})
    result3 = mongo_jobstore.query_one({"uuid": uuid2, "index": 1})

    assert result1["output"] == 11
    assert result2["output"] == "11_end"
    assert result3["output"] == "12345_end"

    # assert job2 (replaced job) ran before job3
    assert result2["completed_at"] < result3["completed_at"]


def test_stop_jobflow_flow(lpad, mongo_jobstore, fw_dir, stop_jobflow_flow, capsys):
    from fireworks.core.rocket_launcher import rapidfire

    from jobflow.managers.fireworks import flow_to_workflow

    flow = stop_jobflow_flow()
    uuid1 = flow.jobs[0].uuid
    uuid2 = flow.jobs[1].uuid

    wf = flow_to_workflow(flow, mongo_jobstore)
    fw_ids = lpad.add_wf(wf)

    # run the workflow
    rapidfire(lpad)

    # check workflow completed
    fw_id = list(fw_ids.values())[0]
    wf = lpad.get_wf_by_fw_id(fw_id)

    assert set(wf.fw_states.values()) == {"COMPLETED", "DEFUSED"}

    # check store has the activity output
    result1 = mongo_jobstore.query_one({"uuid": uuid1})
    result2 = mongo_jobstore.query_one({"uuid": uuid2})

    assert result1["output"] == "1234"
    assert result2 is None


def test_stop_jobflow_job(lpad, mongo_jobstore, fw_dir, stop_jobflow_job, capsys):
    from fireworks.core.rocket_launcher import rapidfire

    from jobflow.managers.fireworks import flow_to_workflow

    job = stop_jobflow_job()
    uuid1 = job.uuid

    wf = flow_to_workflow(job, mongo_jobstore)
    fw_ids = lpad.add_wf(wf)

    # run the workflow
    rapidfire(lpad)

    # check workflow completed
    fw_id = list(fw_ids.values())[0]
    wf = lpad.get_wf_by_fw_id(fw_id)

    assert list(wf.fw_states.values()) == ["COMPLETED"]

    # check store has the activity output
    result1 = mongo_jobstore.query_one({"uuid": uuid1})

    assert result1["output"] == "1234"


def test_stop_children_flow(lpad, mongo_jobstore, fw_dir, stop_children_flow, capsys):
    from fireworks.core.rocket_launcher import rapidfire

    from jobflow.managers.fireworks import flow_to_workflow

    flow = stop_children_flow()
    uuid1 = flow.jobs[0].uuid
    uuid2 = flow.jobs[1].uuid
    uuid3 = flow.jobs[2].uuid

    wf = flow_to_workflow(flow, mongo_jobstore)
    fw_ids = lpad.add_wf(wf)

    # run the workflow
    rapidfire(lpad)

    # check workflow completed
    fw_id = list(fw_ids.values())[0]
    wf = lpad.get_wf_by_fw_id(fw_id)

    assert list(wf.fw_states.values()) == ["COMPLETED", "DEFUSED", "COMPLETED"]

    # check store has the activity output
    result1 = mongo_jobstore.query_one({"uuid": uuid1})
    result2 = mongo_jobstore.query_one({"uuid": uuid2})
    result3 = mongo_jobstore.query_one({"uuid": uuid3})

    assert result1["output"] == "1234"
    assert result2 is None
    assert result3["output"] == "12345_end"


def test_error_flow(lpad, mongo_jobstore, fw_dir, error_flow):
    from fireworks.core.rocket_launcher import rapidfire

    from jobflow.managers.fireworks import flow_to_workflow

    flow = error_flow()
    uuid = flow.jobs[0].uuid

    wf = flow_to_workflow(flow, mongo_jobstore)
    fw_ids = lpad.add_wf(wf)

    # run the workflow
    rapidfire(lpad)
    # check workflow completed
    fw_id = list(fw_ids.values())[0]
    wf = lpad.get_wf_by_fw_id(fw_id)

    assert set(wf.fw_states.values()) == {"WAITING", "FIZZLED"}

    result1 = mongo_jobstore.query_one({"uuid": uuid})

    assert result1 is None


def test_stored_data_flow(lpad, mongo_jobstore, fw_dir, stored_data_flow, capsys):
    from fireworks.core.rocket_launcher import rapidfire

    from jobflow.managers.fireworks import flow_to_workflow

    flow = stored_data_flow()
    flow.jobs[0].uuid

    wf = flow_to_workflow(flow, mongo_jobstore)
    fw_ids = lpad.add_wf(wf)

    # run the workflow
    rapidfire(lpad)

    # check workflow completed
    fw_id = list(fw_ids.values())[0]
    wf = lpad.get_wf_by_fw_id(fw_id)

    assert list(wf.fw_states.values()) == ["COMPLETED"]

    result = lpad.db.launches.find_one({"fw_id": fw_id})
    assert result["action"]["stored_data"] == {"a": "message"}


def test_detour_stop_flow(lpad, mongo_jobstore, fw_dir, detour_stop_flow, capsys):
    from fireworks.core.rocket_launcher import rapidfire

    from jobflow.managers.fireworks import flow_to_workflow

    flow = detour_stop_flow()
    uuid1 = flow.jobs[0].uuid
    uuid3 = flow.jobs[1].uuid

    wf = flow_to_workflow(flow, mongo_jobstore)
    fw_ids = lpad.add_wf(wf)

    # run the workflow
    rapidfire(lpad)

    # check workflow completed
    fw_id = list(fw_ids.values())[0]
    wf = lpad.get_wf_by_fw_id(fw_id)

    uuids = [fw.tasks[0]["job"].uuid for fw in wf.fws]
    uuid2 = [u for u in uuids if u != uuid1 and u != uuid3][0]
    assert list(wf.fw_states.values()) == ["DEFUSED", "COMPLETED", "COMPLETED"]

    # check store has the activity output
    result1 = mongo_jobstore.query_one({"uuid": uuid1})
    result2 = mongo_jobstore.query_one({"uuid": uuid2})
    result3 = mongo_jobstore.query_one({"uuid": uuid3})

    assert result1["output"] == 11
    assert result2["output"] == "1234"
    assert result3 is None


def test_replace_and_detour_flow(
    lpad, mongo_jobstore, fw_dir, replace_and_detour_flow, capsys
):
    from fireworks.core.rocket_launcher import rapidfire

    from jobflow.managers.fireworks import flow_to_workflow

    flow = replace_and_detour_flow()
    uuid1 = flow.jobs[0].uuid
    uuid3 = flow.jobs[1].uuid

    wf = flow_to_workflow(flow, mongo_jobstore)
    fw_ids = lpad.add_wf(wf)

    # run the workflow
    rapidfire(lpad)

    # check workflow completed
    fw_id = list(fw_ids.values())[0]
    wf = lpad.get_wf_by_fw_id(fw_id)

    uuids = [fw.tasks[0]["job"].uuid for fw in wf.fws]
    uuid2 = [u for u in uuids if u != uuid1 and u != uuid3][0]

    assert all([s == "COMPLETED" for s in wf.fw_states.values()])

    # check store has the activity output
    result1 = mongo_jobstore.query_one({"uuid": uuid1, "index": 1})
    result2 = mongo_jobstore.query_one({"uuid": uuid1, "index": 2})
    result3 = mongo_jobstore.query_one({"uuid": uuid2, "index": 1})
    result4 = mongo_jobstore.query_one({"uuid": uuid3, "index": 1})

    assert result1["output"] == 11
    assert result2["output"] == "11_end"
    assert result3["output"] == "xyz_end"
    assert result4["output"] == "12345_end"
