import os
from pathlib import Path

import pytest


def test_simple_job(memory_jobstore, clean_dir, simple_job):
    from jobflow import run_locally

    # run with log
    job = simple_job("12345")
    uuid = job.uuid
    responses = run_locally(job, store=memory_jobstore)

    # check responses has been filled
    response1 = responses[uuid][1]
    assert response1.output == "12345_end"
    # check job_dir
    assert isinstance(response1.job_dir, Path)
    assert os.path.isdir(response1.job_dir)

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
    uuid = flow[0].uuid

    # run without log
    run_locally(flow, store=memory_jobstore, log=False)
    captured = capsys.readouterr()
    assert "INFO Started executing jobs locally" not in captured.out
    assert "INFO Finished executing jobs locally" not in captured.out

    # run with custom log format
    custom_fmt = "%(name)s: %(levelname)s - %(message)s"
    run_locally(flow, store=memory_jobstore, log=custom_fmt)
    stdout, stderr = capsys.readouterr()
    assert "jobflow.managers.local: INFO - Started executing jobs locally" in stdout
    assert "jobflow.managers.local: INFO - Finished executing jobs locally" in stdout
    assert stderr == ""

    # run with log=True
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

    # run with folders and root_dir
    root_dir = "test"
    assert Path(root_dir).exists() is False
    responses = run_locally(
        flow, store=memory_jobstore, create_folders=True, root_dir=root_dir
    )
    assert responses[uuid][1].output == "12345_end"
    folders = list(Path(root_dir).glob("job_*/"))
    assert len(folders) == 1


def test_connected_flow(memory_jobstore, clean_dir, connected_flow):
    from jobflow import run_locally

    flow = connected_flow()
    uuid1 = flow[0].uuid
    uuid2 = flow[1].uuid

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
    uuid1 = flow[0][0].uuid
    uuid2 = flow[0][1].uuid
    uuid3 = flow[1][0].uuid
    uuid4 = flow[1][1].uuid

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
    uuid1 = flow[0].uuid

    # run with log
    responses = run_locally(flow, store=memory_jobstore)
    uuid2 = next(u for u in responses if u != uuid1)

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
    uuid1 = flow[0].uuid
    uuid3 = flow[1].uuid

    # run with log
    responses = run_locally(flow, store=memory_jobstore)
    uuid2 = next(uuid for uuid in responses if uuid not in {uuid1, uuid3})

    # check responses has been filled
    assert len(responses) == 3
    assert responses[uuid1][1].output == 11
    assert responses[uuid1][1].detour is not None
    assert isinstance(responses[uuid1][1].job_dir, Path)
    assert os.path.isdir(responses[uuid1][1].job_dir)
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
    uuid1 = flow[0].uuid
    uuid2 = flow[1].uuid

    # run with log
    responses = run_locally(flow, store=memory_jobstore)

    # check responses has been filled
    assert len(responses) == 2
    assert len(responses[uuid1]) == 2
    assert responses[uuid1][1].output == 11
    assert responses[uuid1][1].replace is not None
    assert isinstance(responses[uuid1][1].job_dir, Path)
    assert os.path.isdir(responses[uuid1][1].job_dir)
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
    uuid1 = flow[0].uuid
    uuid2 = flow[1].uuid

    # run with log
    responses = run_locally(flow, store=memory_jobstore)

    # check responses has been filled
    assert len(responses) == 4
    assert len(responses[uuid1]) == 2
    assert responses[uuid1][1].output == 11
    assert responses[uuid1][1].replace is not None
    assert type(responses[uuid1][2].output["first"]).__name__ == "OutputReference"
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
    uuid1 = flow[0].uuid

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
    uuid1 = flow[0].uuid
    uuid2 = flow[1].uuid
    uuid3 = flow[2].uuid

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

    with pytest.raises(ValueError, match="errored"):
        run_locally(flow, store=memory_jobstore, ensure_success=True)

    with pytest.raises(ValueError, match="errored"):
        run_locally(flow, store=memory_jobstore, raise_immediately=True)


def test_ensure_success_with_replace(memory_jobstore, error_replace_flow, capsys):
    from jobflow import run_locally

    flow = error_replace_flow()

    responses = run_locally(flow, store=memory_jobstore)

    # check responses has been filled with the replaced
    # job's output
    assert len(responses) == 1
    assert flow.job_uuids[0] in responses

    captured = capsys.readouterr()
    assert "error_func failed with exception" in captured.out

    with pytest.raises(ValueError, match="errored"):
        run_locally(flow, store=memory_jobstore, ensure_success=True)


def test_ensure_success_with_detour(error_detour_flow, memory_jobstore, capsys):
    from jobflow import run_locally

    flow = error_detour_flow()

    responses = run_locally(flow, store=memory_jobstore)

    # check responses has been filled with the detour output
    assert len(responses) == 2

    captured = capsys.readouterr()
    assert "error_func failed with exception" in captured.out

    with pytest.raises(ValueError, match="errored"):
        run_locally(flow, store=memory_jobstore, ensure_success=True)


def test_ensure_success_with_addition(error_addition_flow, memory_jobstore, capsys):
    from jobflow import run_locally

    flow = error_addition_flow()

    responses = run_locally(flow, store=memory_jobstore)

    # check responses has been filled with the addition output
    assert len(responses) == 2

    captured = capsys.readouterr()
    assert "error_func failed with exception" in captured.out

    with pytest.raises(ValueError, match="errored"):
        run_locally(flow, store=memory_jobstore, ensure_success=True)


def test_ensure_success_retains_first_exception_and_continues(memory_jobstore):
    import traceback

    from jobflow import Flow, JobOrder, OnMissing, job, run_locally

    caught = []
    events = []
    cause = LookupError("cause")

    @job
    def fail():
        error = ValueError("first failure")
        caught.append(error)
        events.append("failed")
        raise error from cause

    @job
    def continue_job():
        events.append("continued")
        return 17

    @job
    def blocked_job(value):
        events.append("blocked")
        return value

    failed = fail()
    continued = continue_job()
    blocked = blocked_job(failed.output)
    continued.config.on_missing_references = OnMissing.NONE
    blocked.config.on_missing_references = OnMissing.ERROR
    flow = Flow([failed, continued, blocked], order=JobOrder.LINEAR)

    with pytest.raises(ValueError, match="first failure") as raised:
        run_locally(flow, store=memory_jobstore, ensure_success=True)

    assert raised.value is caught[0]
    assert raised.value.__cause__ is cause
    assert "fail" in {
        frame.name for frame in traceback.extract_tb(raised.value.__traceback__)
    }
    assert events == ["failed", "continued"]
    assert memory_jobstore.get_output(continued.uuid) == 17


def test_ensure_success_false_continues_after_failure(memory_jobstore):
    from jobflow import Flow, JobOrder, OnMissing, job, run_locally

    events = []

    @job
    def fail():
        events.append("failed")
        raise ValueError("failure without deferred raise")

    @job
    def continue_job():
        events.append("continued")
        return 23

    failed = fail()
    continued = continue_job()
    continued.config.on_missing_references = OnMissing.NONE
    flow = Flow([failed, continued], order=JobOrder.LINEAR)

    responses = run_locally(flow, store=memory_jobstore, ensure_success=False)

    assert events == ["failed", "continued"]
    assert responses[continued.uuid][1].output == 23
    assert memory_jobstore.get_output(continued.uuid) == 23


def test_deferred_raise_preserves_original_exception_context(memory_jobstore):
    from jobflow import job, run_locally

    inner_errors = []
    job_errors = []
    caller_error = RuntimeError("caller")

    @job
    def fail():
        try:
            raise KeyError("job context")
        except KeyError as inner_error:
            inner_errors.append(inner_error)
            error = ValueError("job failure")
            job_errors.append(error)
            raise error from inner_error

    try:
        raise caller_error
    except RuntimeError:
        with pytest.raises(ValueError, match="job failure") as raised:
            run_locally(fail(), store=memory_jobstore, ensure_success=True)

    assert raised.value is job_errors[0]
    assert raised.value.__context__ is inner_errors[0]
    assert raised.value.__context__ is not caller_error


def test_ensure_success_raises_first_failure_in_execution_order(memory_jobstore):
    from jobflow import Flow, JobOrder, OnMissing, job, run_locally

    caught = []
    events = []

    @job
    def fail(label):
        error = ValueError(label)
        caught.append(error)
        events.append(label)
        raise error

    first = fail("first")
    second = fail("second")
    second.config.on_missing_references = OnMissing.NONE
    flow = Flow([first, second], order=JobOrder.LINEAR)

    with pytest.raises(ValueError, match="first") as raised:
        run_locally(flow, store=memory_jobstore, ensure_success=True)

    assert events == ["first", "second"]
    assert raised.value is caught[0]
    assert raised.value is not caught[1]


@pytest.mark.parametrize("action", ["replace", "detour", "addition"])
def test_ensure_success_defers_dynamic_flow_failures(action, memory_jobstore):
    from jobflow import Flow, JobOrder, OnMissing, Response, job, run_locally

    events = []
    caught = []

    @job
    def fail():
        error = ValueError(f"{action} failure")
        caught.append(error)
        events.append("failed")
        raise error

    @job
    def dynamic_job():
        return Response(output="dynamic", **{action: fail()})

    @job
    def continue_job():
        events.append("continued")

    continued = continue_job()
    continued.config.on_missing_references = OnMissing.NONE
    flow = Flow([dynamic_job(), continued], order=JobOrder.LINEAR)

    with pytest.raises(ValueError, match=f"{action} failure") as raised:
        run_locally(flow, store=memory_jobstore, ensure_success=True)

    assert raised.value is caught[0]
    assert events == ["failed", "continued"]


@pytest.mark.parametrize("ensure_success", [False, True])
def test_raise_immediately_propagates_before_later_jobs(
    ensure_success, memory_jobstore
):
    from jobflow import Flow, JobOrder, OnMissing, job, run_locally

    events = []

    @job
    def fail():
        events.append("failed")
        raise ValueError("immediate failure")

    @job
    def later_job():
        events.append("later")

    later = later_job()
    later.config.on_missing_references = OnMissing.NONE
    flow = Flow([fail(), later], order=JobOrder.LINEAR)

    with pytest.raises(ValueError, match="immediate failure"):
        run_locally(
            flow,
            store=memory_jobstore,
            ensure_success=ensure_success,
            raise_immediately=True,
        )

    assert events == ["failed"]


@pytest.mark.parametrize("flow_fixture", ["stop_jobflow_flow", "stop_children_flow"])
def test_ensure_success_keeps_generic_error_for_stopped_flow(
    flow_fixture, memory_jobstore, request
):
    from jobflow import run_locally

    flow = request.getfixturevalue(flow_fixture)()

    with pytest.raises(RuntimeError, match="Flow did not finish running successfully"):
        run_locally(flow, store=memory_jobstore, ensure_success=True)


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
    uuid1 = flow[0].uuid
    uuid3 = flow[1].uuid

    # run with log
    responses = run_locally(flow, store=memory_jobstore)
    uuid2 = next(u for u in responses if u not in {uuid1, uuid3})

    # check responses has been filled
    assert len(responses) == 2
    assert responses[uuid1][1].output == 11
    assert responses[uuid1][1].detour is not None
    assert responses[uuid1][1].job_dir is None
    assert responses[uuid2][1].output == "1234"
    # TODO maybe find way to set artificial job_dir and test is not None
    assert responses[uuid2][1].job_dir is None

    # check store has the activity output
    result1 = memory_jobstore.query_one({"uuid": uuid1})
    result2 = memory_jobstore.query_one({"uuid": uuid2})
    result3 = memory_jobstore.query_one({"uuid": uuid3})

    assert result1["output"] == 11
    assert result2["output"] == "1234"
    assert result3 is None


def test_external_reference(memory_jobstore, clean_dir, simple_job):
    from jobflow import OutputReference, run_locally

    # run a first job
    job1 = simple_job("12345")
    uuid1 = job1.uuid
    responses = run_locally(job1, store=memory_jobstore)

    # check responses has been filled
    assert responses[uuid1][1].output == "12345_end"

    # run a second job with external reference to the first
    job2 = simple_job(OutputReference(uuid1))
    uuid2 = job2.uuid
    responses = run_locally(job2, store=memory_jobstore, allow_external_references=True)
    assert responses[uuid2][1].output == "12345_end_end"
    assert isinstance(responses[uuid2][1].job_dir, Path)
    assert os.path.isdir(responses[uuid2][1].job_dir)


def test_resolve_references_both(memory_jobstore, clean_dir):
    from jobflow import Flow, JobConfig, job, run_locally
    from jobflow.core.reference import OutputReference, ResolvedReference

    @job
    def initial():
        return 2

    @job
    def verify(arg):
        assert isinstance(arg, ResolvedReference)
        assert isinstance(arg.reference, OutputReference)
        assert arg.value == 2
        return arg.value * 2

    job1 = initial()
    job2 = verify(job1.output)
    job2.config = JobConfig(resolve_references="both")

    responses = run_locally(Flow([job1, job2]), store=memory_jobstore)
    assert responses[job2.uuid][1].output == 4
