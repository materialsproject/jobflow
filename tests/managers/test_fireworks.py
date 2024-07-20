import pytest

pytest.importorskip("fireworks")


def test_flow_to_workflow(
    memory_jobstore, simple_job, simple_flow, connected_flow, nested_flow
):
    from fireworks import Workflow

    from jobflow import OnMissing
    from jobflow.managers.fireworks import flow_to_workflow

    # test simple job
    flow = simple_job()
    wf = flow_to_workflow(flow, memory_jobstore)

    assert type(wf) is Workflow
    assert wf.name == "Flow"
    assert len(wf.fws) == 1
    assert wf.fws[0].name == "func"

    # test simple job no store with custom name
    flow = simple_job()
    wf = flow_to_workflow(flow, name="custom_name")

    assert type(wf) is Workflow
    assert wf.name == "custom_name"
    assert len(wf.fws) == 1
    assert wf.fws[0].name == "func"

    # test simple flow
    flow = simple_flow()
    wf = flow_to_workflow(flow, memory_jobstore)

    assert type(wf) is Workflow
    assert wf.name == "Flow"
    assert len(wf.fws) == 1
    assert wf.fws[0].name == "func"

    # test connected flow
    flow = connected_flow()
    wf = flow_to_workflow(flow, memory_jobstore)

    assert type(wf) is Workflow
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
    flow[0].config.on_missing_references = OnMissing.NONE
    wf = flow_to_workflow(flow, memory_jobstore)

    assert wf.fws[0].spec["_allow_fizzled_parents"] is True

    # test manager config
    flow = connected_flow()
    flow[0].config.manager_config = {"metadata": 5}
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

    assert type(fw) is Firework
    assert fw.name == "func"

    job2 = simple_job()
    fw = job_to_firework(
        job2, memory_jobstore, parents=[job.uuid], parent_mapping={job.uuid: 1}
    )

    assert type(fw) is Firework
    assert fw.name == "func"

    with pytest.raises(ValueError, match="Both or neither of"):
        job_to_firework(job2, memory_jobstore, parents=[job.uuid])


def test_simple_flow(lpad, mongo_jobstore, fw_dir, simple_flow, capsys):
    from fireworks.core.rocket_launcher import rapidfire

    from jobflow.managers.fireworks import flow_to_workflow

    flow = simple_flow()
    uuid = flow[0].uuid

    wf = flow_to_workflow(flow, mongo_jobstore)
    fw_ids = lpad.add_wf(wf)

    # run the workflow
    rapidfire(lpad)

    # check workflow completed
    fw_id = next(iter(fw_ids.values()))
    wf = lpad.get_wf_by_fw_id(fw_id)

    assert all(s == "COMPLETED" for s in wf.fw_states.values())

    # check store has the activity output
    result = mongo_jobstore.query_one({"uuid": uuid})
    assert result["output"] == "12345_end"

    # check logs printed
    captured = capsys.readouterr()
    assert "INFO Starting job - func" in captured.out
    assert "INFO Finished job - func" in captured.out


def test_simple_flow_no_store(lpad, fw_dir, simple_flow, capsys):
    from fireworks.core.rocket_launcher import rapidfire

    from jobflow import SETTINGS
    from jobflow.managers.fireworks import flow_to_workflow

    flow = simple_flow()
    uuid = flow[0].uuid

    wf = flow_to_workflow(flow)
    fw_ids = lpad.add_wf(wf)

    # run the workflow
    rapidfire(lpad)

    # check workflow completed
    fw_id = next(iter(fw_ids.values()))
    wf = lpad.get_wf_by_fw_id(fw_id)

    assert all(s == "COMPLETED" for s in wf.fw_states.values())

    # check store has the activity output
    result = SETTINGS.JOB_STORE.query_one({"uuid": uuid})
    assert result["output"] == "12345_end"

    # check logs printed
    captured = capsys.readouterr()
    assert "INFO Starting job - func" in captured.out
    assert "INFO Finished job - func" in captured.out


def test_simple_flow_metadata(
    lpad, mongo_jobstore, fw_dir, simple_flow, connected_flow, capsys
):
    from fireworks.core.rocket_launcher import rapidfire

    from jobflow.managers.fireworks import flow_to_workflow

    flow = simple_flow()
    uuid = flow[0].uuid
    flow[0].metadata = {"tags": ["my_flow"]}

    wf = flow_to_workflow(flow, mongo_jobstore)
    fw_ids = lpad.add_wf(wf)

    # run the workflow
    rapidfire(lpad)

    # check workflow completed
    fw_id = next(iter(fw_ids.values()))
    wf = lpad.get_wf_by_fw_id(fw_id)

    assert all(s == "COMPLETED" for s in wf.fw_states.values())
    assert wf.fws[0].spec["tags"] == ["my_flow"]

    # check store has the activity output
    result = mongo_jobstore.query_one({"uuid": uuid})
    assert result["output"] == "12345_end"
    assert result["metadata"]["fw_id"] == fw_id
    assert result["metadata"]["tags"] == ["my_flow"]

    # test override
    flow = simple_flow()
    flow[0].config.manager_config = {"_add_launchpad_and_fw_id": False}
    uuid = flow[0].uuid

    wf = flow_to_workflow(flow, mongo_jobstore)
    lpad.add_wf(wf)

    # run the workflow
    rapidfire(lpad)

    result = mongo_jobstore.query_one({"uuid": uuid})
    assert result["metadata"] == {}

    # Test flow with metadata added after conversion to workflow
    # (for example: if an atomate powerup is used to add metadata)
    flow = simple_flow()
    uuid = flow[0].uuid
    wf = flow_to_workflow(flow, mongo_jobstore)
    wf.metadata = ["my_flow"]
    for idx_fw in range(len(wf.fws)):
        wf.fws[idx_fw].spec["tags"] = ["my_flow"]

    fw_ids = lpad.add_wf(wf)

    # run the workflow
    rapidfire(lpad)

    result = mongo_jobstore.query_one({"uuid": uuid})
    fw_id = next(iter(fw_ids.values()))
    assert result["metadata"] == {"fw_id": fw_id, "tags": ["my_flow"]}

    # Test flow with existing tags
    flow = connected_flow()
    flow[0].metadata["tags"] = "some tag"
    uuid0 = flow[0].uuid
    flow[1].metadata["tags"] = ["tag, you're it"]
    uuid1 = flow[1].uuid
    wf = flow_to_workflow(flow, mongo_jobstore)
    wf.metadata = ["my_flow"]
    for idx_fw in range(len(wf.fws)):
        wf.fws[idx_fw].spec["tags"] = ["my_flow"]

    fw_ids = lpad.add_wf(wf)

    # run the workflow
    rapidfire(lpad)

    result = mongo_jobstore.query_one({"uuid": uuid0})
    assert result["metadata"]["tags"] == ["some tag", "my_flow"]

    result = mongo_jobstore.query_one({"uuid": uuid1})
    assert result["metadata"]["tags"] == ["tag, you're it", "my_flow"]


def test_connected_flow(lpad, mongo_jobstore, fw_dir, connected_flow, capsys):
    from fireworks.core.rocket_launcher import rapidfire

    from jobflow.managers.fireworks import flow_to_workflow

    flow = connected_flow()
    uuid1 = flow[0].uuid
    uuid2 = flow[1].uuid

    wf = flow_to_workflow(flow, mongo_jobstore)
    fw_ids = lpad.add_wf(wf)

    # run the workflow
    rapidfire(lpad)

    # check workflow completed
    fw_id = next(iter(fw_ids.values()))
    wf = lpad.get_wf_by_fw_id(fw_id)

    assert all(s == "COMPLETED" for s in wf.fw_states.values())

    # check store has the activity output
    result1 = mongo_jobstore.query_one({"uuid": uuid1})
    result2 = mongo_jobstore.query_one({"uuid": uuid2})

    assert result1["output"] == "12345_end"
    assert result2["output"] == "12345_end_end"


def test_nested_flow(lpad, mongo_jobstore, fw_dir, nested_flow, capsys):
    from fireworks.core.rocket_launcher import rapidfire

    from jobflow.managers.fireworks import flow_to_workflow

    flow = nested_flow()
    uuid1 = flow[0][0].uuid
    uuid2 = flow[0][1].uuid
    uuid3 = flow[1][0].uuid
    uuid4 = flow[1][1].uuid

    wf = flow_to_workflow(flow, mongo_jobstore)
    fw_ids = lpad.add_wf(wf)

    # run the workflow
    rapidfire(lpad)

    # check workflow completed
    fw_id = next(iter(fw_ids.values()))
    wf = lpad.get_wf_by_fw_id(fw_id)

    assert all(s == "COMPLETED" for s in wf.fw_states.values())

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
    uuid1 = flow[0].uuid

    wf = flow_to_workflow(flow, mongo_jobstore)
    fw_ids = lpad.add_wf(wf)

    # run the workflow
    rapidfire(lpad)

    # check workflow completed
    fw_id = next(iter(fw_ids.values()))
    wf = lpad.get_wf_by_fw_id(fw_id)

    uuids = [fw.tasks[0]["job"].uuid for fw in wf.fws]
    uuid2 = next(u for u in uuids if u != uuid1)
    assert all(s == "COMPLETED" for s in wf.fw_states.values())

    # check store has the activity output
    result1 = mongo_jobstore.query_one({"uuid": uuid1})
    result2 = mongo_jobstore.query_one({"uuid": uuid2})

    assert result1["output"] == 11
    assert result2["output"] == "11_end"


def test_detour_flow(lpad, mongo_jobstore, fw_dir, detour_flow, capsys):
    from fireworks.core.rocket_launcher import rapidfire

    from jobflow.managers.fireworks import flow_to_workflow

    flow = detour_flow()
    uuid1 = flow[0].uuid
    uuid3 = flow[1].uuid

    wf = flow_to_workflow(flow, mongo_jobstore)
    fw_ids = lpad.add_wf(wf)

    # run the workflow
    rapidfire(lpad)

    # check workflow completed
    fw_id = next(iter(fw_ids.values()))
    wf = lpad.get_wf_by_fw_id(fw_id)

    uuids = [fw.tasks[0]["job"].uuid for fw in wf.fws]
    uuid2 = next(u for u in uuids if u not in {uuid1, uuid3})
    assert all(s == "COMPLETED" for s in wf.fw_states.values())

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
    uuid1 = flow[0].uuid
    uuid2 = flow[1].uuid

    wf = flow_to_workflow(flow, mongo_jobstore)
    fw_ids = lpad.add_wf(wf)

    # run the workflow
    rapidfire(lpad)

    # check workflow completed
    fw_id = next(iter(fw_ids.values()))
    wf = lpad.get_wf_by_fw_id(fw_id)

    assert all(s == "COMPLETED" for s in wf.fw_states.values())

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
    uuid1 = flow[0].uuid
    uuid2 = flow[1].uuid

    wf = flow_to_workflow(flow, mongo_jobstore)
    fw_ids = lpad.add_wf(wf)

    # run the workflow
    rapidfire(lpad)

    # check workflow completed
    fw_id = next(iter(fw_ids.values()))
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
    fw_id = next(iter(fw_ids.values()))
    wf = lpad.get_wf_by_fw_id(fw_id)

    assert list(wf.fw_states.values()) == ["COMPLETED"]

    # check store has the activity output
    result1 = mongo_jobstore.query_one({"uuid": uuid1})

    assert result1["output"] == "1234"


def test_stop_children_flow(lpad, mongo_jobstore, fw_dir, stop_children_flow, capsys):
    from collections import Counter

    from fireworks.core.rocket_launcher import rapidfire

    from jobflow.managers.fireworks import flow_to_workflow

    flow = stop_children_flow()
    uuid1 = flow[0].uuid
    uuid2 = flow[1].uuid
    uuid3 = flow[2].uuid

    wf = flow_to_workflow(flow, mongo_jobstore)
    fw_ids = lpad.add_wf(wf)

    # run the workflow
    rapidfire(lpad)

    # check workflow completed
    fw_id = next(iter(fw_ids.values()))
    wf = lpad.get_wf_by_fw_id(fw_id)

    states = Counter(wf.fw_states.values())
    assert states["COMPLETED"] == 2
    assert states["DEFUSED"] == 1

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
    uuid = flow[0].uuid

    wf = flow_to_workflow(flow, mongo_jobstore)
    fw_ids = lpad.add_wf(wf)

    # run the workflow
    rapidfire(lpad)
    # check workflow completed
    fw_id = next(iter(fw_ids.values()))
    wf = lpad.get_wf_by_fw_id(fw_id)

    assert set(wf.fw_states.values()) == {"WAITING", "FIZZLED"}

    result1 = mongo_jobstore.query_one({"uuid": uuid})

    assert result1 is None


def test_stored_data_flow(lpad, mongo_jobstore, fw_dir, stored_data_flow, capsys):
    from fireworks.core.rocket_launcher import rapidfire

    from jobflow.managers.fireworks import flow_to_workflow

    flow = stored_data_flow()
    _fw_id = flow[0].uuid

    wf = flow_to_workflow(flow, mongo_jobstore)
    fw_ids = lpad.add_wf(wf)

    # run the workflow
    rapidfire(lpad)

    # check workflow completed
    fw_id = next(iter(fw_ids.values()))
    wf = lpad.get_wf_by_fw_id(fw_id)

    assert list(wf.fw_states.values()) == ["COMPLETED"]

    result = lpad.db.launches.find_one({"fw_id": fw_id})
    assert result["action"]["stored_data"] == {"a": "message"}


def test_detour_stop_flow(lpad, mongo_jobstore, fw_dir, detour_stop_flow, capsys):
    from fireworks.core.rocket_launcher import rapidfire

    from jobflow.managers.fireworks import flow_to_workflow

    flow = detour_stop_flow()
    uuid1 = flow[0].uuid
    uuid3 = flow[1].uuid

    wf = flow_to_workflow(flow, mongo_jobstore)
    fw_ids = lpad.add_wf(wf)

    # run the workflow
    rapidfire(lpad)

    # check workflow completed
    fw_id = next(iter(fw_ids.values()))
    wf = lpad.get_wf_by_fw_id(fw_id)

    uuids = [fw.tasks[0]["job"].uuid for fw in wf.fws]
    uuid2 = next(u for u in uuids if u not in {uuid1, uuid3})

    # Sort by firework id explicitly instead of assuming they are sorted
    states_dict = dict(zip(list(wf.id_fw.keys()), list(wf.fw_states.values())))
    sorted_states_dict = dict(sorted(states_dict.items()))
    assert list(sorted_states_dict.values()) == ["DEFUSED", "COMPLETED", "COMPLETED"]

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
    uuid1 = flow[0].uuid
    uuid3 = flow[1].uuid

    wf = flow_to_workflow(flow, mongo_jobstore)
    fw_ids = lpad.add_wf(wf)

    # run the workflow
    rapidfire(lpad)

    # check workflow completed
    fw_id = next(iter(fw_ids.values()))
    wf = lpad.get_wf_by_fw_id(fw_id)

    uuids = [fw.tasks[0]["job"].uuid for fw in wf.fws]
    uuid2 = next(u for u in uuids if u not in {uuid1, uuid3})

    assert all(s == "COMPLETED" for s in wf.fw_states.values())

    # check store has the activity output
    result1 = mongo_jobstore.query_one({"uuid": uuid1, "index": 1})
    result2 = mongo_jobstore.query_one({"uuid": uuid1, "index": 2})
    result3 = mongo_jobstore.query_one({"uuid": uuid2, "index": 1})
    result4 = mongo_jobstore.query_one({"uuid": uuid3, "index": 1})

    assert result1["output"] == 11
    assert result2["output"] == "11_end"
    assert result3["output"] == "xyz_end"
    assert result4["output"] == "12345_end"


def test_external_reference(lpad, mongo_jobstore, fw_dir, simple_job, capsys):
    from fireworks.core.rocket_launcher import rapidfire

    from jobflow import Flow, OutputReference
    from jobflow.managers.fireworks import flow_to_workflow

    # run a first flow
    job1 = simple_job("12345")
    uuid1 = job1.uuid
    flow1 = Flow([job1])
    wf1 = flow_to_workflow(flow1, mongo_jobstore)
    fw_ids = lpad.add_wf(wf1)

    # run the workflow
    rapidfire(lpad)

    # check workflow completed
    fw_id = next(iter(fw_ids.values()))
    wf1 = lpad.get_wf_by_fw_id(fw_id)
    assert all(s == "COMPLETED" for s in wf1.fw_states.values())

    # run a second flow with external reference to the first
    job2 = simple_job(OutputReference(uuid1))
    uuid2 = job2.uuid
    flow2 = Flow([job2])
    wf2 = flow_to_workflow(flow2, mongo_jobstore, allow_external_references=True)
    fw_ids = lpad.add_wf(wf2)

    # run the workflow
    rapidfire(lpad)

    # check workflow completed
    fw_id = next(iter(fw_ids.values()))
    wf2 = lpad.get_wf_by_fw_id(fw_id)
    assert all(s == "COMPLETED" for s in wf2.fw_states.values())

    # check response
    result2 = mongo_jobstore.query_one({"uuid": uuid2})
    assert result2["output"] == "12345_end_end"


def test_maker_flow(lpad, mongo_jobstore, fw_dir, maker_with_callable, capsys):
    from fireworks.core.rocket_launcher import rapidfire

    from jobflow.core.flow import Flow
    from jobflow.managers.fireworks import flow_to_workflow

    j = maker_with_callable(f=sum).make(a=1, b=2)

    flow = Flow([j])
    uuid = flow[0].uuid

    wf = flow_to_workflow(flow, mongo_jobstore)
    fw_ids = lpad.add_wf(wf)

    # run the workflow
    rapidfire(lpad)

    # check workflow completed
    fw_id = next(iter(fw_ids.values()))
    wf = lpad.get_wf_by_fw_id(fw_id)

    assert all(s == "COMPLETED" for s in wf.fw_states.values())

    # check store has the activity output
    result = mongo_jobstore.query_one({"uuid": uuid})
    assert result["output"] == 3

    # check logs printed
    captured = capsys.readouterr()
    assert "INFO Starting job - TestCallableMaker" in captured.out
    assert "INFO Finished job - TestCallableMaker" in captured.out
