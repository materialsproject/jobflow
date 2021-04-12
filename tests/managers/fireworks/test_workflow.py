from typing import List

from flows import Activity, Response, job


@job
def simple_task(message):
    return message + "_end"


def simple_activity():
    simple = simple_task("12345")
    return Activity("simple_activity", [simple], simple.output)


def test_activity_to_workflow(mongo_store):
    from fireworks import Workflow

    from flows.managers.fireworks import activity_to_workflow

    activity = simple_activity()
    wf = activity_to_workflow(activity, mongo_store)

    assert type(wf) == Workflow
    assert wf.name == "simple_activity"
    assert len(wf.fws) == 2
    assert wf.fws[0].name == "simple_task"
    assert wf.fws[1].name == "simple_activity to store"


def test_fireworks_integration(lpad, mongo_store, clean_dir):
    from fireworks.core.rocket_launcher import rapidfire

    from flows.managers.fireworks import activity_to_workflow

    activity = simple_activity()
    wf = activity_to_workflow(activity, mongo_store)
    fw_ids = lpad.add_wf(wf)

    # run the workflow
    rapidfire(lpad)

    # check workflow completed
    fw_id = list(fw_ids.values())[0]
    wf = lpad.get_wf_by_fw_id(fw_id)

    assert all([s == "COMPLETED" for s in wf.fw_states.values()])

    # check output_store has the activity output
    result = mongo_store.query_one({"uuid": str(activity.uuid)})
    assert result["output"] == "12345_end"


@job
def read_websites():
    return ["https://youtube.com", "https://google.com"]


@job
def time_website(website: str):
    return sum(map(ord, website))


@job
def detour_timing_jobs(websites: List[str]):
    jobs = [time_website(website) for website in websites]
    output = [j.output for j in jobs]
    detour_activity = Activity("timings", jobs, output)
    return Response(restart=detour_activity)


@job
def sum_times(times: List[float]):
    return sum(times)


def get_detour_activity_test():
    # create an activity that will first load a list of websites, then generate new
    # jobs to calculate the time taken to load each website, and finally, sum all the
    # times together

    read_job = read_websites()
    detour_job = detour_timing_jobs(read_job.output)
    sum_job = sum_times(detour_job.output)
    my_activity = Activity(
        "time websites",
        [read_job, detour_job, sum_job],
        sum_job.output,
    )
    return my_activity


def test_fireworks_detour(lpad, mongo_store, clean_dir):
    from fireworks.core.rocket_launcher import rapidfire

    from flows.managers.fireworks import activity_to_workflow

    activity = get_detour_activity_test()
    wf = activity_to_workflow(activity, mongo_store)
    fw_ids = lpad.add_wf(wf)

    # run the workflow
    rapidfire(lpad)

    # check workflow completed
    fw_id = list(fw_ids.values())[0]
    wf = lpad.get_wf_by_fw_id(fw_id)

    assert all([s == "COMPLETED" for s in wf.fw_states.values()])

    # check output_store has the activity output
    result = mongo_store.query_one({"uuid": str(activity.uuid)})
    assert result["output"] == 3578


@job
def fibonacci_restart(smaller: int, larger: int, stop_point: int = 1000):
    total = smaller + larger

    if total > stop_point:
        return total

    new_job = fibonacci_restart(larger, total, stop_point=stop_point)
    return Response(output=total, restart=new_job)


@job
def fibonacci_addition(smaller: int, larger: int, stop_point: int = 1000):
    total = smaller + larger

    if total > stop_point:
        return total

    new_job = fibonacci_addition(larger, total, stop_point=stop_point)
    return Response(output=total, addition=new_job)


def test_fireworks_restart(lpad, mongo_store, clean_dir):
    from fireworks.core.rocket_launcher import rapidfire

    from flows.managers.fireworks import activity_to_workflow

    fibonacci_job = fibonacci_addition(1, 1)
    wf = activity_to_workflow(fibonacci_job, mongo_store)
    fw_ids = lpad.add_wf(wf)

    # run the workflow
    rapidfire(lpad)

    # check workflow completed
    fw_id = list(fw_ids.values())[0]
    wf = lpad.get_wf_by_fw_id(fw_id)

    assert len(wf.fw_states) == 15
    assert all([s == "COMPLETED" for s in wf.fw_states.values()])

    # check output_store has the activity output
    result = mongo_store.query_one(sort={"completed_at": -1})
    assert result["output"] == 1597


def test_fireworks_addition(lpad, mongo_store, clean_dir):
    from fireworks.core.rocket_launcher import rapidfire

    from flows.managers.fireworks import activity_to_workflow

    fibonacci_job = fibonacci_addition(1, 1)
    wf = activity_to_workflow(fibonacci_job, mongo_store)
    fw_ids = lpad.add_wf(wf)

    # run the workflow
    rapidfire(lpad)

    # check workflow completed
    fw_id = list(fw_ids.values())[0]
    wf = lpad.get_wf_by_fw_id(fw_id)

    assert len(wf.fw_states) == 15
    assert all([s == "COMPLETED" for s in wf.fw_states.values()])

    # check output_store has the activity output
    result = mongo_store.query_one(sort={"completed_at": -1})
    assert result["output"] == 1597
