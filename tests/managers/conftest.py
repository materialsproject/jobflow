"""This module provides helper fixtures for quickly generating flows and jobs."""

import pytest


@pytest.fixture(scope="session")
def simple_job():
    from jobflow import job

    global func

    @job
    def func(message):
        return message + "_end"

    return func


@pytest.fixture(scope="session")
def simple_flow(simple_job):
    from jobflow import Flow

    def _gen():
        simple = simple_job("12345")
        return Flow([simple], simple.output)

    return _gen


@pytest.fixture(scope="session")
def connected_flow(simple_job):
    from jobflow import Flow

    def _gen():
        simple1 = simple_job("12345")
        simple2 = simple_job(simple1.output)
        return Flow([simple1, simple2], simple2.output, "Connected Flow")

    return _gen


@pytest.fixture(scope="session")
def nested_flow(connected_flow):
    from jobflow import Flow

    def _gen():
        flow1 = connected_flow()
        flow2 = connected_flow()
        flow2.jobs[0].function_args = (flow1.jobs[1].output,)
        return Flow([flow1, flow2], flow2.output)

    return _gen


@pytest.fixture(scope="session")
def addition_job(simple_job):
    from jobflow import Response, job

    global addition_func

    @job
    def addition_func(a, b):
        return Response(output=a + b, addition=simple_job(str(a + b)))

    return addition_func


@pytest.fixture(scope="session")
def addition_flow(addition_job):
    from jobflow import Flow

    def _gen():
        add = addition_job(5, 6)
        return Flow([add], add.output)

    return _gen


@pytest.fixture(scope="session")
def detour_job(simple_job):
    from jobflow import Response, job

    global detour_func

    @job
    def detour_func(a, b):
        return Response(output=a + b, detour=simple_job(str(a + b)))

    return detour_func


@pytest.fixture(scope="session")
def detour_flow(detour_job, simple_job):
    from jobflow import Flow, JobOrder

    def _gen():
        detour = detour_job(5, 6)
        simple = simple_job("12345")
        return Flow([detour, simple], simple.output, order=JobOrder.LINEAR)

    return _gen


@pytest.fixture(scope="session")
def replace_job(simple_job):
    from jobflow import Response, job

    global replace_func

    @job
    def replace_func(a, b):
        return Response(output=a + b, replace=simple_job(str(a + b)))

    return replace_func


@pytest.fixture(scope="session")
def replace_job_with_flow(simple_job):
    from jobflow import Flow, Response, job

    global replace_func_flow

    @job
    def replace_func_flow(a, b):
        first_job = simple_job(str(a + b))
        second_job = simple_job(first_job.output)
        flow = Flow(
            [first_job, second_job],
            {"first": first_job.output, "second": second_job.output},
        )
        return Response(output=a + b, replace=flow)

    return replace_func_flow


@pytest.fixture(scope="session")
def replace_flow(replace_job, simple_job):
    from jobflow import Flow, JobOrder

    def _gen():
        replace = replace_job(5, 6)
        simple = simple_job("12345")
        return Flow([replace, simple], simple.output, order=JobOrder.LINEAR)

    return _gen


@pytest.fixture(scope="session")
def replace_flow_nested(replace_job_with_flow, simple_job):
    from jobflow import Flow, JobOrder

    def _gen():
        replace = replace_job_with_flow(5, 6)
        simple = simple_job("12345")
        return Flow([replace, simple], simple.output, order=JobOrder.LINEAR)

    return _gen


@pytest.fixture(scope="session")
def stop_jobflow_job():
    from jobflow import Response, job

    global stop_jobflow_func

    @job
    def stop_jobflow_func():
        return Response(output="1234", stop_jobflow=True)

    return stop_jobflow_func


@pytest.fixture(scope="session")
def stop_jobflow_flow(stop_jobflow_job, simple_job):
    from jobflow import Flow, JobOrder

    def _gen():
        stop = stop_jobflow_job()
        simple = simple_job("12345")
        return Flow([stop, simple], simple.output, order=JobOrder.LINEAR)

    return _gen


@pytest.fixture(scope="session")
def stop_children_job():
    from jobflow import Response, job

    global stop_children_func

    @job
    def stop_children_func():
        return Response(output="1234", stop_children=True)

    return stop_children_func


@pytest.fixture(scope="session")
def stop_children_flow(stop_children_job, simple_job):
    from jobflow import Flow

    def _gen():
        stop = stop_children_job()
        simple1 = simple_job(stop.output)
        simple2 = simple_job("12345")
        return Flow([stop, simple1, simple2], simple2.output)

    return _gen


@pytest.fixture(scope="session")
def error_job():
    from jobflow import job

    global error_func

    @job
    def error_func():
        raise ValueError("errored")

    return error_func


@pytest.fixture(scope="session")
def error_flow(error_job, simple_job):
    from jobflow import Flow

    def _gen():
        error = error_job()
        simple1 = simple_job(error.output)
        simple2 = simple_job(simple1.output)
        return Flow([error, simple1, simple2])

    return _gen


@pytest.fixture(scope="session")
def stored_data_job():
    from jobflow import Response, job

    global stored_data_func

    @job
    def stored_data_func(message):
        return Response(output=message + "_end", stored_data={"a": "message"})

    return stored_data_func


@pytest.fixture(scope="session")
def stored_data_flow(stored_data_job):
    from jobflow import Flow

    def _gen():
        store = stored_data_job("12345")
        return Flow([store])

    return _gen


@pytest.fixture(scope="session")
def detour_stop_job(stop_jobflow_job):
    from jobflow import Response, job

    global detour_stop_func

    @job
    def detour_stop_func(a, b):
        return Response(output=a + b, detour=stop_jobflow_job())

    return detour_stop_func


@pytest.fixture(scope="session")
def detour_stop_flow(detour_stop_job, simple_job):
    from jobflow import Flow, JobOrder

    def _gen():
        detour = detour_stop_job(5, 6)
        simple = simple_job("12345")
        return Flow([detour, simple], simple.output, order=JobOrder.LINEAR)

    return _gen


@pytest.fixture(scope="session")
def fw_dir():
    # longer session
    import os
    import shutil
    import tempfile

    old_cwd = os.getcwd()
    newpath = tempfile.mkdtemp()
    os.chdir(newpath)

    yield

    os.chdir(old_cwd)
    shutil.rmtree(newpath)


@pytest.fixture(scope="session")
def replace_and_detour_job(simple_job):
    from jobflow import Response, job

    global replace_and_detour_func

    @job
    def replace_and_detour_func(a, b):
        return Response(
            output=a + b, replace=simple_job(str(a + b)), detour=simple_job("xyz")
        )

    return replace_and_detour_func


@pytest.fixture(scope="session")
def replace_and_detour_flow(replace_and_detour_job, simple_job):
    from jobflow import Flow, JobOrder

    def _gen():
        replace = replace_and_detour_job(5, 6)
        simple = simple_job("12345")
        return Flow([replace, simple], simple.output, order=JobOrder.LINEAR)

    return _gen
