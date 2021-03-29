import pytest


def get_task():
    from activities import Task
    return Task(function=("builtins", "sum"), args=(1, 2))


def test_activity_of_tasks_init():
    from activities.core.activity import Activity
    from activities.core.outputs import Value, Dynamic
    from uuid import UUID

    # test empty activity
    activity = Activity()
    assert isinstance(activity.uuid, UUID)
    assert activity.host is None
    assert activity.outputs is None
    assert activity.task_type == "task"

    # test single task
    add_task = get_task()
    activity = Activity(name="Add", tasks=[add_task])
    assert activity.name == 'Add'
    assert activity.host is None
    assert activity.outputs is None
    assert activity.task_type == "task"

    # test multiple task
    add_task1 = get_task()
    add_task2 = get_task()
    activity = Activity(tasks=[add_task1, add_task2])
    assert activity.host is None
    assert activity.outputs is None
    assert activity.task_type == "task"

    # test single task and outputs
    add_task = get_task()
    activity = Activity(tasks=[add_task], outputs=add_task.outputs)
    assert isinstance(activity.outputs, type(add_task.outputs))
    assert activity.outputs is not add_task.outputs
    assert activity.output_sources is add_task.outputs

    # test single task and reference output
    add_task = get_task()
    activity = Activity(tasks=[add_task], outputs=add_task.outputs.value)
    assert isinstance(activity.outputs, Value)

    # test multi task and list multi outputs
    add_task1 = get_task()
    add_task2 = get_task()
    activity = Activity(
        tasks=[add_task1, add_task2], outputs=[add_task1.outputs, add_task2.outputs]
    )
    assert isinstance(activity.outputs, Value)

    # dict outputs
    add_task = get_task()
    activity = Activity(
        tasks=[add_task], outputs={"a": add_task.outputs, "b": add_task.outputs}
    )
    assert isinstance(activity.outputs, Dynamic)

    # test all tasks included needed to generate outputs
    add_task = get_task()
    with pytest.raises(ValueError):
        Activity(tasks=[], outputs=add_task.outputs)

    # test task given rather than outputs
    with pytest.raises(ValueError):
        add_task = get_task()
        Activity(tasks=[add_task], outputs=add_task)

    # test list of tasks given rather than outputs
    with pytest.raises(ValueError):
        add_task = get_task()
        Activity(tasks=[add_task], outputs=[add_task])

    # test dict of tasks given rather than outputs
    with pytest.raises(ValueError):
        add_task = get_task()
        Activity(tasks=[add_task], outputs={"a": add_task})

    # test complex object containing task given rather than outputs
    with pytest.raises(ValueError):
        add_task = get_task()
        Activity(tasks=[add_task], outputs={1: [[{"a": add_task}]]})


def test_activity_of_activities_init():
    from activities.core.activity import Activity
    from activities.core.outputs import Value, Dynamic

    # test single activity
    add_task = get_task()
    subactivity = Activity(tasks=[add_task])
    activity = Activity(name="Add", tasks=[subactivity])
    assert activity.name == 'Add'
    assert activity.host is None
    assert activity.outputs is None
    assert activity.task_type == "activity"
    assert activity.tasks[0].host == activity.uuid

    # test multiple activities
    add_task1 = get_task()
    subactivity1 = Activity(tasks=[add_task1])
    add_task2 = get_task()
    subactivity2 = Activity(tasks=[add_task2])
    activity = Activity(tasks=[subactivity1, subactivity2])
    assert activity.host is None
    assert activity.outputs is None
    assert activity.task_type == "activity"
    assert activity.tasks[0].host == activity.uuid
    assert activity.tasks[1].host == activity.uuid

    # test single task and outputs
    add_task = get_task()
    subactivity = Activity(tasks=[add_task])
    activity = Activity(name="Add", tasks=[subactivity], outputs=subactivity.outputs)
    assert isinstance(activity.outputs, type(add_task.outputs))
    assert isinstance(activity.outputs, type(subactivity.outputs))
    assert activity.outputs is not add_task.outputs
    assert activity.outputs is not subactivity.outputs
    assert activity.output_sources is not add_task.outputs
    assert activity.output_sources is subactivity.outputs

    # test single task and reference output
    add_task = get_task()
    subactivity = Activity(tasks=[add_task])
    activity = Activity(name="Add", tasks=[subactivity], outputs=subactivity.outputs.value)
    assert isinstance(activity.outputs, Value)

    # test multi task and list multi outputs
    add_task1 = get_task()
    subactivity1 = Activity(tasks=[add_task1])
    add_task2 = get_task()
    subactivity2 = Activity(tasks=[add_task2])
    activity = Activity(
        tasks=[subactivity1, subactivity2],
        outputs=[subactivity1.outputs, subactivity2.outputs.value]
    )
    assert isinstance(activity.outputs, Value)

    # dict outputs
    add_task = get_task()
    subactivity = Activity(tasks=[add_task])
    activity = Activity(
        tasks=[subactivity],
        outputs={"a": subactivity.outputs, "b": subactivity.outputs.value}
    )
    assert isinstance(activity.outputs, Dynamic)

    # test all activities included needed to generate outputs
    add_task = get_task()
    subactivity = Activity(tasks=[add_task])
    with pytest.raises(ValueError):
        Activity(tasks=[], outputs=subactivity.outputs)

    # test activity given rather than outputs
    with pytest.raises(ValueError):
        add_task = get_task()
        subactivity = Activity(tasks=[add_task])
        Activity(tasks=[subactivity], outputs=subactivity)

    # test list of activity given rather than outputs
    with pytest.raises(ValueError):
        add_task = get_task()
        subactivity = Activity(tasks=[add_task])
        Activity(tasks=[subactivity], outputs=[subactivity])

    # test dict of activity given rather than outputs
    with pytest.raises(ValueError):
        add_task = get_task()
        subactivity = Activity(tasks=[add_task])
        Activity(tasks=[subactivity], outputs={"a": subactivity})

    # test complex object containing task given rather than outputs
    with pytest.raises(ValueError):
        add_task = get_task()
        subactivity = Activity(tasks=[add_task])
        Activity(tasks=[subactivity], outputs={1: [[{"a": subactivity}]]})


def test_activity_sharing():
    from activities import Activity

    # test that activities cannot be shared between multiple activities
    add_task = get_task()
    shared_activity = Activity(tasks=[add_task])
    activity = Activity(tasks=[shared_activity])
    assert activity

    with pytest.raises(ValueError):
        Activity(tasks=[shared_activity])


def test_task_sharing():
    from activities import Activity

    # test that tasks cannot be shared between multiple activities
    shared_task = get_task()
    activity = Activity(tasks=[shared_task])
    assert activity

    with pytest.raises(ValueError):
        Activity(tasks=[shared_task])


def test_task_multiplicity():
    from activities import Activity

    # test that two of the same task cannot be used in the same activity
    add_task = get_task()
    with pytest.raises(ValueError):
        Activity(tasks=[add_task, add_task])


def test_activity_multiplicity():
    from activities import Activity

    # test that two of the same activity cannot be used in the same activity
    add_task = get_task()
    activity = Activity(tasks=[add_task])

    with pytest.raises(ValueError):
        Activity(tasks=[activity, activity])


def test_task_dag_validation():
    from activities import Activity
    from activities import Task

    # test tasks out of order
    task1 = Task(function=("builtins", "sum"), args=(1, 2))
    task2 = Task(function=("builtins", "sum"), args=(task1.outputs.value, 2))
    activity = Activity(tasks=[task2, task1])
    with pytest.raises(ValueError):
        activity.validate()

    # test cycle detection of tasks
    task1 = Task(function=("builtins", "sum"), args=(1, 2))
    task2 = Task(function=("builtins", "sum"), args=(task1.outputs.value, 2))
    task1.args = (task2.outputs.value, 2)
    activity = Activity(tasks=[task1, task2])
    with pytest.raises(ValueError):
        activity.validate()

    # test all tasks included for graph to work
    task1 = Task(function=("builtins", "sum"), args=(1, 2))
    task2 = Task(function=("builtins", "sum"), args=(task1.outputs.value, 2))
    activity = Activity(tasks=[task2])
    with pytest.raises(ValueError):
        activity.validate()


def test_activity_dag_validation():
    from activities import Activity
    from activities import Task

    # test cycle detection of sub-activities
    task1 = Task(function=("builtins", "sum"), args=(1, 2))
    subactivity1 = Activity(tasks=[task1])

    task2 = Task(function=("builtins", "sum"), args=(subactivity1.outputs.value, 2))
    subactivity2 = Activity(tasks=[add_task2])
    activity = Activity(tasks=[subactivity1, subactivity2])
    with pytest.raises(ValueError):
        activity.validate()

    # other things to test:
    # - DAG for activities and tasks
    # - test all tasks (activities or tasks) included for graph to work
    # - test all the above bad inputs but for task Task object args and kwargs
    # - add validate call inside either task_to_wf or iteractivity


def test_serialization():
    import json
    from uuid import uuid4

    from monty.json import MontyDecoder, MontyEncoder

    activity = Activity("MyActivity", [])
    activity_host = Activity("MyActivity", [activity])
    host_uuid = activity_host.uuid

    encoded_activity = json.loads(MontyEncoder().encode(activity_host))
    decoded_activity = MontyDecoder().process_decoded(encoded_activity)

    assert decoded_activity.tasks[0].host == host_uuid
