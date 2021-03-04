from dataclasses import dataclass
from activities import task, Outputs


@dataclass
class SimpleOutput(Outputs):
    message: str


@task(outputs=SimpleOutput)
def simple_task(message):
    return SimpleOutput(message=message + "_end")


def simple_activity():
    simple = simple_task("12345")
    return Activity("simple activity", [simple], simple.outputs)


def test_activity_to_workflow(output_store):
    from activities.managers.fireworks.workflow import activity_to_workflow
    from fireworks import Workflow

    activity = simple_activity()
    wf = activity_to_workflow(activity, output_store)

    assert type(wf) == Workflow
    assert wf.name == "simple activity"
    assert len(wf.fws) == 1
    assert wf.fws[0].name == "simple activity"


def test_fireworks_integration(lpad, output_store, clean_dir):
    from activities.managers.fireworks.workflow import activity_to_workflow
    from fireworks.core.rocket_launcher import rapidfire

    activity = simple_activity()
    wf = activity_to_workflow(activity, output_store)
    fw_ids = lpad.add_wf(wf)

    # run the workflow
    rapidfire(lpad)

    # check workflow completed
    fw_id = list(fw_ids.values())[0]
    wf = lpad.get_wf_by_fw_id(fw_id)

    assert all([s == 'COMPLETED' for s in wf.fw_states.values()])

    # check output_store has the activity output
    output_store.connect()
    outputs = output_store.query_one({"uuid": str(activity.uuid)})
    assert outputs["message"] == "12345_end"


from dataclasses import dataclass
from typing import List
from activities import Outputs, task, Activity, Detour


@dataclass
class ListOfWebsites(Outputs):
    websites: List[str]


@task(outputs=ListOfWebsites)
def read_websites():
    return ListOfWebsites(["https://youtube.com", "https://google.com"])


# define a task, outputs and activity to calculate how long it takes to load a website

@dataclass
class TimeTaken(Outputs):
    time: float


@task(outputs=TimeTaken)
def time_website(website: str):
    import urllib.request
    from time import perf_counter

    stream = urllib.request.urlopen(website)
    start_time = perf_counter()
    stream.read()
    end_time = perf_counter()
    stream.close()
    return TimeTaken(end_time - start_time)


def get_time_activity(website: str):
    website_load = time_website(website)
    return Activity("Time", [website_load], website_load.outputs)


# define a task and outputs to generate time activities for multiple websites

@dataclass
class ListOfTimeTaken(Outputs):
    times: List[float]


@task(outputs=ListOfTimeTaken)
def detour_timing_activities(websites: List[str]):
    activities = [get_time_activity(website) for website in websites]
    outputs = ListOfTimeTaken([activity.outputs.time for activity in activities])
    detour_activity = Activity("Timings", activities, outputs)
    return Detour(detour_activity)


# define a task to sum multiple times together

@task(outputs=TimeTaken)
def sum_times(times: List[float]):
    total_time = sum(times)
    return TimeTaken(total_time)


def get_detour_activity_test():
    # create an activity that will first load a list of websites, then generate new
    # activities to calculate the time taken to load each website, and finally, sum all
    # the times together

    load_websites_task = read_websites()
    detour_activities = detour_timing_activities(load_websites_task.outputs.websites)
    sum_task = sum_times(detour_activities.outputs.times)
    my_activity = Activity(
        "Time websites",
        [load_websites_task, detour_activities, sum_task],
        sum_task.outputs
    )
    return my_activity


def test_fireworks_detour(lpad, output_store, clean_dir):
    from activities.managers.fireworks.workflow import activity_to_workflow
    from fireworks.core.rocket_launcher import rapidfire

    activity = get_detour_activity_test()
    wf = activity_to_workflow(activity, output_store)
    fw_ids = lpad.add_wf(wf)

    # run the workflow
    rapidfire(lpad)

    # check workflow completed
    fw_id = list(fw_ids.values())[0]
    wf = lpad.get_wf_by_fw_id(fw_id)

    assert all([s == 'COMPLETED' for s in wf.fw_states.values()])

    # check output_store has the activity output
    output_store.connect()
    outputs = output_store.query_one({"uuid": str(activity.uuid)})
    assert outputs["time"] > 0
