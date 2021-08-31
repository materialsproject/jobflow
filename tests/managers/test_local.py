def test_simple_job(memory_jobstore, clean_dir, simple_job):
    from jobflow import run_locally

    # run with log
    job = simple_job("12345")
    uuid = job.uuid
    responses = run_locally(job, store=memory_jobstore)

    # check responses has been filled
    assert responses[uuid][1].output == "12345_end"

    # check store has the activity output
    result = memory_jobstore.query_one({"uuid": uuid})
    assert result["output"] == "12345_end"

    # test run no store
    job = simple_job("12345")
    uuid = job.uuid
    responses = run_locally(job)
    assert responses[uuid][1].output == "12345_end"


def test_simple_flow(memory_jobstore, clean_dir, simple_flow, capsys):
    from pathlib import Path

    from jobflow import run_locally

    flow = simple_flow()
    uuid = flow.jobs[0].uuid

    # run without log
    run_locally(flow, store=memory_jobstore, log=False)
    captured = capsys.readouterr()
    assert "INFO Started executing jobs locally" not in captured.out
    assert "INFO Finished executing jobs locally" not in captured.out

    # run with log
    responses = run_locally(flow, store=memory_jobstore)

    # check responses has been filled
    assert responses[uuid][1].output == "12345_end"

    # check store has the activity output
    result = memory_jobstore.query_one({"uuid": uuid})
    assert result["output"] == "12345_end"

    # check no folders were written
    folders = list(Path(".").glob("job_*/"))
    assert len(folders) == 0

    # check logs printed
    captured = capsys.readouterr()
    assert "INFO Started executing jobs locally" in captured.out
    assert "INFO Finished executing jobs locally" in captured.out

    # run with folders
    responses = run_locally(flow, store=memory_jobstore, create_folders=True)
    assert responses[uuid][1].output == "12345_end"
    folders = list(Path(".").glob("job_*/"))
    assert len(folders) == 1


def test_connected_flow(memory_jobstore, clean_dir, connected_flow):
    from jobflow import run_locally

    flow = connected_flow()
    uuid1 = flow.jobs[0].uuid
    uuid2 = flow.jobs[1].uuid

    # run with log
    responses = run_locally(flow, store=memory_jobstore)

    # check responses has been filled
    assert len(responses) == 2
    assert responses[uuid1][1].output == "12345_end"
    assert responses[uuid2][1].output == "12345_end_end"

    # check store has the activity output
    result1 = memory_jobstore.query_one({"uuid": uuid1})
    result2 = memory_jobstore.query_one({"uuid": uuid2})

    assert result1["output"] == "12345_end"
    assert result2["output"] == "12345_end_end"


def test_nested_flow(memory_jobstore, clean_dir, nested_flow):
    from jobflow import run_locally

    flow = nested_flow()
    uuid1 = flow.jobs[0].jobs[0].uuid
    uuid2 = flow.jobs[0].jobs[1].uuid
    uuid3 = flow.jobs[1].jobs[0].uuid
    uuid4 = flow.jobs[1].jobs[1].uuid

    # run with log
    responses = run_locally(flow, store=memory_jobstore)

    # check responses has been filled
    assert len(responses) == 4
    assert responses[uuid1][1].output == "12345_end"
    assert responses[uuid2][1].output == "12345_end_end"
    assert responses[uuid3][1].output == "12345_end_end_end"
    assert responses[uuid4][1].output == "12345_end_end_end_end"

    # check store has the activity output
    result1 = memory_jobstore.query_one({"uuid": uuid1})
    result2 = memory_jobstore.query_one({"uuid": uuid2})
    result3 = memory_jobstore.query_one({"uuid": uuid3})
    result4 = memory_jobstore.query_one({"uuid": uuid4})

    assert result1["output"] == "12345_end"
    assert result2["output"] == "12345_end_end"
    assert result3["output"] == "12345_end_end_end"
    assert result4["output"] == "12345_end_end_end_end"


def test_addition_flow(memory_jobstore, clean_dir, addition_flow):
    from jobflow import run_locally

    flow = addition_flow()
    uuid1 = flow.jobs[0].uuid

    # run with log
    responses = run_locally(flow, store=memory_jobstore)
    uuid2 = [u for u in responses.keys() if u != uuid1][0]

    # check responses has been filled
    assert len(responses) == 2
    assert responses[uuid1][1].output == 11
    assert responses[uuid1][1].addition is not None
    assert responses[uuid2][1].output == "11_end"

    # check store has the activity output
    result1 = memory_jobstore.query_one({"uuid": uuid1})
    result2 = memory_jobstore.query_one({"uuid": uuid2})

    assert result1["output"] == 11
    assert result2["output"] == "11_end"


def test_detour_flow(memory_jobstore, clean_dir, detour_flow):
    from jobflow import run_locally

    flow = detour_flow()
    uuid1 = flow.jobs[0].uuid
    uuid3 = flow.jobs[1].uuid

    # run with log
    responses = run_locally(flow, store=memory_jobstore)
    uuid2 = [u for u in responses.keys() if u != uuid1 and u != uuid3][0]

    # check responses has been filled
    assert len(responses) == 3
    assert responses[uuid1][1].output == 11
    assert responses[uuid1][1].detour is not None
    assert responses[uuid2][1].output == "11_end"
    assert responses[uuid3][1].output == "12345_end"

    # check store has the activity output
    result1 = memory_jobstore.query_one({"uuid": uuid1})
    result2 = memory_jobstore.query_one({"uuid": uuid2})
    result3 = memory_jobstore.query_one({"uuid": uuid3})

    assert result1["output"] == 11
    assert result2["output"] == "11_end"
    assert result3["output"] == "12345_end"

    # assert job2 (detoured job) ran before job3
    assert result2["completed_at"] < result3["completed_at"]


def test_replace_flow(memory_jobstore, clean_dir, replace_flow):
    from jobflow import run_locally

    flow = replace_flow()
    uuid1 = flow.jobs[0].uuid
    uuid2 = flow.jobs[1].uuid

    # run with log
    responses = run_locally(flow, store=memory_jobstore)

    # check responses has been filled
    assert len(responses) == 2
    assert len(responses[uuid1]) == 2
    assert responses[uuid1][1].output == 11
    assert responses[uuid1][1].replace is not None
    assert responses[uuid1][2].output == "11_end"
    assert responses[uuid2][1].output == "12345_end"

    # check store has the activity output
    result1 = memory_jobstore.query_one({"uuid": uuid1, "index": 1})
    result2 = memory_jobstore.query_one({"uuid": uuid1, "index": 2})
    result3 = memory_jobstore.query_one({"uuid": uuid2, "index": 1})

    assert result1["output"] == 11
    assert result2["output"] == "11_end"
    assert result3["output"] == "12345_end"

    # assert job2 (replaced job) ran before job3
    assert result2["completed_at"] < result3["completed_at"]


def test_replace_flow_nested(memory_jobstore, clean_dir, replace_flow_nested):
    from jobflow import run_locally

    flow = replace_flow_nested()
    uuid1 = flow.jobs[0].uuid
    uuid2 = flow.jobs[1].uuid

    # run with log
    responses = run_locally(flow, store=memory_jobstore)

    # check responses has been filled
    assert len(responses) == 4
    assert len(responses[uuid1]) == 2
    assert responses[uuid1][1].output == 11
    assert responses[uuid1][1].replace is not None
    assert responses[uuid1][2].output["first"].__class__.__name__ == "OutputReference"
    assert responses[uuid2][1].output == "12345_end"

    # check store has the activity output
    result1 = memory_jobstore.query_one({"uuid": uuid1, "index": 1})
    result2 = memory_jobstore.query_one({"uuid": uuid1, "index": 2})
    result3 = memory_jobstore.query_one({"uuid": uuid2, "index": 1})

    assert result1["output"] == 11
    assert result2["output"]["first"]["@class"] == "OutputReference"
    assert result3["output"] == "12345_end"

    # assert job2 (replaced job) ran before job3
    assert result2["completed_at"] < result3["completed_at"]


def test_stop_jobflow_flow(memory_jobstore, clean_dir, stop_jobflow_flow):
    from jobflow import run_locally

    flow = stop_jobflow_flow()
    uuid1 = flow.jobs[0].uuid

    # run with log
    responses = run_locally(flow, store=memory_jobstore)

    # check responses has been filled
    assert len(responses) == 1
    assert len(responses[uuid1]) == 1
    assert responses[uuid1][1].output == "1234"
    assert responses[uuid1][1].stop_jobflow is True

    # check store has the activity output
    result1 = memory_jobstore.query_one({"uuid": uuid1})

    assert result1["output"] == "1234"


def test_stop_jobflow_job(memory_jobstore, clean_dir, stop_jobflow_job):
    from jobflow import run_locally

    job = stop_jobflow_job()
    uuid1 = job.uuid

    # run with log
    responses = run_locally(job, store=memory_jobstore)

    # check responses has been filled
    assert len(responses) == 1
    assert len(responses[uuid1]) == 1
    assert responses[uuid1][1].output == "1234"
    assert responses[uuid1][1].stop_jobflow is True

    # check store has the activity output
    result1 = memory_jobstore.query_one({"uuid": uuid1})

    assert result1["output"] == "1234"


def test_stop_children_flow(memory_jobstore, clean_dir, stop_children_flow):
    from jobflow import run_locally

    flow = stop_children_flow()
    uuid1 = flow.jobs[0].uuid
    uuid2 = flow.jobs[1].uuid
    uuid3 = flow.jobs[2].uuid

    # run with log
    responses = run_locally(flow, store=memory_jobstore)

    # check responses has been filled
    assert len(responses) == 2
    assert len(responses[uuid1]) == 1
    assert uuid2 not in responses
    assert responses[uuid1][1].output == "1234"
    assert responses[uuid1][1].stop_children is True
    assert responses[uuid3][1].output == "12345_end"

    # check store has the activity output
    result1 = memory_jobstore.query_one({"uuid": uuid1})
    result2 = memory_jobstore.query_one({"uuid": uuid2})
    result3 = memory_jobstore.query_one({"uuid": uuid3})

    assert result1["output"] == "1234"
    assert result2 is None
    assert result3["output"] == "12345_end"


def test_error_flow(memory_jobstore, clean_dir, error_flow, capsys):
    from jobflow import run_locally

    flow = error_flow()

    # run with log
    responses = run_locally(flow, store=memory_jobstore)

    # check responses has been filled
    assert len(responses) == 0

    captured = capsys.readouterr()
    assert "error_func failed with exception" in captured.out


def test_stored_data_flow(memory_jobstore, clean_dir, stored_data_flow, capsys):
    from jobflow import run_locally

    flow = stored_data_flow()

    responses = run_locally(flow, store=memory_jobstore)
    captured = capsys.readouterr()

    # check responses has been filled
    assert len(responses) == 1
    assert "Response.stored_data is not supported" in captured.out


def test_detour_stop_flow(memory_jobstore, clean_dir, detour_stop_flow):
    from jobflow import run_locally

    flow = detour_stop_flow()
    uuid1 = flow.jobs[0].uuid
    uuid3 = flow.jobs[1].uuid

    # run with log
    responses = run_locally(flow, store=memory_jobstore)
    uuid2 = [u for u in responses.keys() if u != uuid1 and u != uuid3][0]

    # check responses has been filled
    assert len(responses) == 2
    assert responses[uuid1][1].output == 11
    assert responses[uuid1][1].detour is not None
    assert responses[uuid2][1].output == "1234"

    # check store has the activity output
    result1 = memory_jobstore.query_one({"uuid": uuid1})
    result2 = memory_jobstore.query_one({"uuid": uuid2})
    result3 = memory_jobstore.query_one({"uuid": uuid3})

    assert result1["output"] == 11
    assert result2["output"] == "1234"
    assert result3 is None
