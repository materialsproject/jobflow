from dataclasses import dataclass

from activities import task, Outputs, Activity


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


def test_fireworks_integration(lpad, output_store):
    from activities.managers.fireworks.workflow import activity_to_workflow
    from fireworks.core.rocket_launcher import rapidfire

    activity = simple_activity()
    wf = activity_to_workflow(activity, output_store)
    lpad.add_wf(wf)

    # run the workflow
    rapidfire(lpad)

    wf = lpad.get_wf_by_fw_id(1)
    assert all([s == 'COMPLETED' for s in wf.fw_states.values()])

