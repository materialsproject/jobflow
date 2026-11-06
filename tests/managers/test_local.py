import logging
import os
import threading
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

    with pytest.raises(RuntimeError):
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

    with pytest.raises(RuntimeError, match="Flow did not finish running successfully"):
        run_locally(flow, store=memory_jobstore, ensure_success=True)


def test_ensure_success_with_detour(error_detour_flow, memory_jobstore, capsys):
    from jobflow import run_locally

    flow = error_detour_flow()

    responses = run_locally(flow, store=memory_jobstore)

    # check responses has been filled with the detour output
    assert len(responses) == 2

    captured = capsys.readouterr()
    assert "error_func failed with exception" in captured.out

    with pytest.raises(RuntimeError, match="Flow did not finish running successfully"):
        run_locally(flow, store=memory_jobstore, ensure_success=True)


def test_ensure_success_with_addition(error_addition_flow, memory_jobstore, capsys):
    from jobflow import run_locally

    flow = error_addition_flow()

    responses = run_locally(flow, store=memory_jobstore)

    # check responses has been filled with the addition output
    assert len(responses) == 2

    captured = capsys.readouterr()
    assert "error_func failed with exception" in captured.out

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


def _make_log_job(label, store, inner_log=None, fail_after_inner=False):
    from jobflow import job, run_locally

    @job
    def task():
        if inner_log is not None:
            inner = _make_log_job(f"inner-{label}", store)
            run_locally(inner, log=inner_log, store=store)
        logging.getLogger("host.application").warning("HOST:%s", label)
        print(f"PRINT:{label}")
        if fail_after_inner:
            raise ValueError("expected outer failure")
        return label

    result = task()
    result.name = label
    return result


def _root_handler_state(root):
    return (
        root.level,
        tuple(
            (
                id(handler),
                handler.level,
                repr(handler.formatter),
                tuple(map(id, handler.filters)),
            )
            for handler in root.handlers
        ),
    )


def test_log_false_first_preserves_root_handler(
    memory_jobstore, caplog, capsys, monkeypatch
):
    from jobflow import run_locally

    package_logger = logging.getLogger("jobflow")
    root_logger = logging.getLogger()
    old_handlers = package_logger.handlers
    old_level = package_logger.level
    monkeypatch.setattr(package_logger, "handlers", [])
    monkeypatch.setattr(package_logger, "level", logging.NOTSET)
    caplog.set_level(logging.INFO)
    root_before = _root_handler_state(root_logger)

    responses = run_locally(
        _make_log_job("false-first", memory_jobstore),
        log=False,
        store=memory_jobstore,
    )

    assert len(responses) == 1
    assert next(iter(responses.values()))[1].output == "false-first"
    assert not any(record.name.startswith("jobflow.") for record in caplog.records)
    assert any(record.name == "host.application" for record in caplog.records)
    assert "PRINT:false-first" in capsys.readouterr().out
    assert package_logger.handlers == []
    assert package_logger.level == logging.NOTSET
    assert _root_handler_state(root_logger) == root_before
    monkeypatch.setattr(package_logger, "handlers", old_handlers)
    monkeypatch.setattr(package_logger, "level", old_level)


def test_log_false_after_true_suppresses_internal_logs(memory_jobstore, caplog, capsys):
    from jobflow import run_locally

    caplog.set_level(logging.INFO)
    first = _make_log_job("first", memory_jobstore)
    run_locally(first, log=True, store=memory_jobstore)
    capsys.readouterr()
    caplog.clear()

    package_logger = logging.getLogger("jobflow")
    root_logger = logging.getLogger()
    logger_before = (
        tuple(map(id, package_logger.handlers)),
        package_logger.level,
        package_logger.propagate,
        _root_handler_state(root_logger),
    )
    responses = run_locally(
        _make_log_job("disabled", memory_jobstore), log=False, store=memory_jobstore
    )
    logger_after = (
        tuple(map(id, package_logger.handlers)),
        package_logger.level,
        package_logger.propagate,
        _root_handler_state(root_logger),
    )

    assert len(responses) == 1
    assert next(iter(responses.values()))[1].output == "disabled"
    assert not any(record.name.startswith("jobflow.") for record in caplog.records)
    assert [(record.name, record.getMessage()) for record in caplog.records] == [
        ("host.application", "HOST:disabled")
    ]
    captured = capsys.readouterr()
    assert captured.out == "PRINT:disabled\n"
    assert captured.err == ""
    assert logger_after == logger_before

    caplog.clear()
    custom_format = "CUSTOM:%(message)s"
    custom = _make_log_job("custom", memory_jobstore)
    run_locally(custom, log=custom_format, store=memory_jobstore)
    captured = capsys.readouterr()
    assert "CUSTOM:Started executing jobs locally" in captured.out
    assert "CUSTOM:Starting job - custom" in captured.out
    assert "CUSTOM:Finished executing jobs locally" in captured.out
    assert any(record.name == "jobflow.core.job" for record in caplog.records)

    caplog.clear()
    empty_format = _make_log_job("empty-format", memory_jobstore)
    run_locally(empty_format, log="", store=memory_jobstore)
    captured = capsys.readouterr()
    assert "CUSTOM:Started executing jobs locally" in captured.out
    assert any(record.name == "jobflow.core.job" for record in caplog.records)

    caplog.clear()
    default = _make_log_job("default-again", memory_jobstore)
    run_locally(default, log=True, store=memory_jobstore)
    captured = capsys.readouterr()
    assert "INFO Started executing jobs locally" in captured.out
    assert "INFO Finished executing jobs locally" in captured.out
    assert any(record.name == "jobflow.core.job" for record in caplog.records)


def test_nested_run_log_arguments_restore_outer_context(
    memory_jobstore, caplog, capsys
):
    from jobflow import run_locally

    caplog.set_level(logging.INFO)
    outer_true = _make_log_job("outer-true", memory_jobstore, inner_log=False)
    run_locally(outer_true, log=True, store=memory_jobstore)
    true_records = [
        record for record in caplog.records if record.name.startswith("jobflow.")
    ]
    assert (
        sum(
            record.getMessage() == "Started executing jobs locally"
            for record in true_records
        )
        == 1
    )
    assert (
        sum(record.getMessage().startswith("Starting job -") for record in true_records)
        == 1
    )
    assert any(
        record.getMessage() == "Finished executing jobs locally"
        for record in true_records
    )
    assert any(record.getMessage() == "HOST:outer-true" for record in caplog.records)
    assert "PRINT:inner-outer-true" in capsys.readouterr().out

    caplog.clear()
    outer_false = _make_log_job("outer-false", memory_jobstore, inner_log=True)
    run_locally(outer_false, log=False, store=memory_jobstore)
    false_records = [
        record for record in caplog.records if record.name.startswith("jobflow.")
    ]
    assert [
        record.getMessage()
        for record in false_records
        if record.name == "jobflow.managers.local"
    ] == [
        "Started executing jobs locally",
        "Finished executing jobs locally",
    ]
    inner_start_records = [
        record.getMessage()
        for record in false_records
        if record.name == "jobflow.core.job"
        and record.getMessage().startswith("Starting job -")
    ]
    assert len(inner_start_records) == 1
    assert inner_start_records[0].startswith("Starting job - inner-outer-false (")
    assert any(record.getMessage() == "HOST:outer-false" for record in caplog.records)
    capsys.readouterr()

    caplog.clear()
    failing = _make_log_job(
        "outer-failure", memory_jobstore, inner_log=True, fail_after_inner=True
    )
    with pytest.raises(ValueError, match="expected outer failure"):
        run_locally(failing, log=False, store=memory_jobstore, raise_immediately=True)
    capsys.readouterr()
    caplog.clear()

    direct = _make_log_job("direct-after-failure", memory_jobstore)
    direct.run(store=memory_jobstore)
    direct_records = [
        record for record in caplog.records if record.name == "jobflow.core.job"
    ]
    assert any(
        record.getMessage().startswith("Starting job -") for record in direct_records
    )
    assert any(
        record.getMessage().startswith("Finished job -") for record in direct_records
    )
    assert "PRINT:direct-after-failure" in capsys.readouterr().out


def test_log_false_does_not_suppress_thread_record(memory_jobstore, caplog):
    from jobflow import job, run_locally

    caplog.set_level(logging.INFO)
    started = threading.Event()
    release = threading.Event()

    @job
    def wait_for_log():
        started.set()
        if not release.wait(5):
            raise TimeoutError("thread logger did not release the job")
        return "finished"

    def emit_from_thread():
        if started.wait(5):
            logging.getLogger("jobflow.core.job").warning("THREAD:ordinary")
        release.set()

    worker = threading.Thread(target=emit_from_thread)
    worker.start()
    responses = run_locally(wait_for_log(), log=False, store=memory_jobstore)
    worker.join(5)

    assert not worker.is_alive()
    assert len(responses) == 1
    records = [(record.name, record.getMessage()) for record in caplog.records]
    assert records == [("jobflow.core.job", "THREAD:ordinary")]
