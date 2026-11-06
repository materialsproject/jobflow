import logging
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


class RecordingHandler(logging.Handler):
    def __init__(self):
        super().__init__()
        self.messages = []
        self.closed_by_manager = False

    def emit(self, record):
        self.messages.append(self.format(record))

    def close(self):
        self.closed_by_manager = True
        super().close()


@pytest.fixture
def preserve_jobflow_logger():
    logger = logging.getLogger("jobflow")
    handlers = logger.handlers[:]
    level = logger.level
    propagate = logger.propagate
    logger.handlers = []
    logger.setLevel(logging.INFO)
    logger.propagate = True
    yield logger
    logger.handlers = handlers
    logger.setLevel(level)
    logger.propagate = propagate


def test_run_locally_routes_custom_handlers(
    memory_jobstore, clean_dir, preserve_jobflow_logger, capsys
):
    from jobflow import job, run_locally

    @job
    def emit_records():
        logging.getLogger("jobflow.tests").info("job info")
        logging.getLogger("jobflow.tests").warning("job warning")
        return "complete"

    logger = preserve_jobflow_logger
    previous = RecordingHandler()
    logger.addHandler(previous)
    info_handler = RecordingHandler()
    warning_handler = RecordingHandler()
    unformatted_handler = RecordingHandler()
    info_handler.setLevel(logging.INFO)
    warning_handler.setLevel(logging.WARNING)
    levels = (info_handler.level, warning_handler.level, unformatted_handler.level)
    info_formatter = logging.Formatter("INFO-HANDLER %(levelname)s %(message)s")
    warning_formatter = logging.Formatter("WARNING-HANDLER %(levelname)s %(message)s")
    info_handler.setFormatter(info_formatter)
    warning_handler.setFormatter(warning_formatter)
    supplied = [info_handler, warning_handler, unformatted_handler]
    registered_handlers = tuple(supplied)

    task = emit_records()
    task.name = "custom-handler-job"
    responses = run_locally(task, store=memory_jobstore, log=supplied)
    supplied.clear()
    logger.addHandler(info_handler)
    logger.info("after successful run")

    assert responses[next(iter(responses))][1].output == "complete"
    assert logger.handlers == [info_handler, warning_handler, unformatted_handler]
    assert info_handler.messages.count("INFO-HANDLER INFO job info") == 1
    assert "WARNING-HANDLER INFO job info" not in warning_handler.messages
    assert warning_handler.messages.count("WARNING-HANDLER WARNING job warning") == 1
    assert "INFO-HANDLER WARNING job warning" in info_handler.messages
    assert "INFO-HANDLER INFO Started executing jobs locally" in info_handler.messages
    assert "INFO-HANDLER INFO Finished executing jobs locally" in info_handler.messages
    assert any(
        item.startswith(f"Starting job - {task.name} (")
        for item in unformatted_handler.messages
    )
    assert any(
        item.startswith(f"Finished job - {task.name} (")
        for item in unformatted_handler.messages
    )
    assert "after successful run" in unformatted_handler.messages
    assert info_handler.formatter is info_formatter
    assert warning_handler.formatter is warning_formatter
    assert unformatted_handler.formatter is None
    assert (
        info_handler.level,
        warning_handler.level,
        unformatted_handler.level,
    ) == levels
    assert all(not handler.closed_by_manager for handler in registered_handlers)
    assert previous.messages == []
    stdout, _ = capsys.readouterr()
    assert stdout == ""


def test_run_locally_empty_handlers_replaces_package_handlers(
    memory_jobstore, clean_dir, preserve_jobflow_logger
):
    from jobflow import job, run_locally

    @job
    def emit_record():
        logging.getLogger("jobflow.tests").info("empty-list sentinel")
        return "complete"

    logger = preserve_jobflow_logger
    previous = RecordingHandler()
    logger.addHandler(previous)
    root = logging.getLogger()
    root_handler = RecordingHandler()
    original_root_handlers = root.handlers[:]
    root_level = root.level
    root.addHandler(root_handler)
    try:
        responses = run_locally(emit_record(), store=memory_jobstore, log=[])
        assert responses[next(iter(responses))][1].output == "complete"
        assert logger.handlers == []
        assert previous.messages == []
        assert any("empty-list sentinel" in item for item in root_handler.messages)
        assert root.level == root_level
        assert root.handlers == [*original_root_handlers, root_handler]
    finally:
        root.handlers = original_root_handlers
        root.setLevel(root_level)


def test_run_locally_log_empty_string_keeps_configuration(
    memory_jobstore, clean_dir, preserve_jobflow_logger, simple_job
):
    from jobflow import run_locally

    logger = preserve_jobflow_logger
    sentinel = RecordingHandler()
    logger.addHandler(sentinel)

    job = simple_job("control")
    run_locally(job, store=memory_jobstore, log="")

    assert logger.handlers == [sentinel]


def test_run_locally_keeps_custom_handlers_after_job_error(
    memory_jobstore, clean_dir, preserve_jobflow_logger, error_job, simple_job
):
    from jobflow import run_locally

    logger = preserve_jobflow_logger
    handler = RecordingHandler()
    handler.setFormatter(None)
    handlers = [handler]

    with pytest.raises(ValueError, match="errored"):
        run_locally(
            error_job(),
            store=memory_jobstore,
            log=handlers,
            raise_immediately=True,
        )

    logger.info("after job error")
    assert logger.handlers == [handler]
    assert handler.formatter is None
    assert "after job error" in handler.messages[-1]
    assert handler.closed_by_manager is False

    run_locally(simple_job("replacement"), store=memory_jobstore, log=True)

    assert handler not in logger.handlers
    assert handler.closed_by_manager is False


def test_run_locally_nested_custom_handlers_remain_selected(
    memory_jobstore, clean_dir, preserve_jobflow_logger
):
    from jobflow import job, run_locally

    inner_handler = RecordingHandler()

    @job
    def inner_job():
        logging.getLogger("jobflow.tests").info("inner job sentinel")
        return "inner"

    @job
    def outer_job():
        run_locally(inner_job(), store=memory_jobstore, log=[inner_handler])
        logging.getLogger("jobflow.tests").info("outer after inner")
        return "outer"

    outer_handler = RecordingHandler()
    responses = run_locally(outer_job(), store=memory_jobstore, log=[outer_handler])

    assert responses[next(iter(responses))][1].output == "outer"
    assert any("inner job sentinel" in item for item in inner_handler.messages)
    assert any("outer after inner" in item for item in inner_handler.messages)
    assert logging.getLogger("jobflow").handlers == [inner_handler]
