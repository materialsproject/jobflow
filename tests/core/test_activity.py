import pytest


def get_task():
    from jobflow import Task

    return Task(function=("builtins", "sum"), args=(1, 2))


def test_activity_of_tasks_init():
    from uuid import UUID

    from jobflow.core.activity import Activity
    from jobflow.core.outputs import Dynamic, Value

    # test empty activity
    activity = Activity()
    assert isinstance(activity.uuid, UUID)
    assert activity.host is None
    assert activity.outputs is None
    assert activity.task_type == "job"

    # test single job
    add_task = get_task()
    activity = Activity(name="Add", tasks=[add_task])
    assert activity.name == "Add"
    assert activity.host is None
    assert activity.outputs is None
    assert activity.task_type == "job"

    # test multiple job
    add_task1 = get_task()
    add_task2 = get_task()
    activity = Activity(tasks=[add_task1, add_task2])
    assert activity.host is None
    assert activity.outputs is None
    assert activity.task_type == "job"

    # test single job and outputs
    add_task = get_task()
    activity = Activity(tasks=[add_task], outputs=add_task.outputs)
    assert isinstance(activity.outputs, type(add_task.outputs))
    assert activity.outputs is not add_task.outputs
    assert activity.output_source is add_task.outputs

    # test single job and reference output
    add_task = get_task()
    activity = Activity(tasks=[add_task], outputs=add_task.outputs.value)
    assert isinstance(activity.outputs, Value)

    # test multi job and list multi outputs
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

    # test job given rather than outputs
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

    # test complex object containing job given rather than outputs
    with pytest.raises(ValueError):
        add_task = get_task()
        Activity(tasks=[add_task], outputs={1: [[{"a": add_task}]]})


def test_activity_of_activities_init():
    from jobflow.core.activity import Activity
    from jobflow.core.outputs import Dynamic, Value

    # test single activity
    add_task = get_task()
    subactivity = Activity(tasks=[add_task])
    activity = Activity(name="Add", tasks=[subactivity])
    assert activity.name == "Add"
    assert activity.host is None
    assert activity.outputs is None
    assert activity.task_type == "activity"
    assert activity.tasks[0].host == activity.uuid

    # test multiple jobflow
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

    # test single job and outputs
    add_task = get_task()
    subactivity = Activity(tasks=[add_task])
    activity = Activity(name="Add", tasks=[subactivity], outputs=subactivity.outputs)
    assert isinstance(activity.outputs, type(add_task.outputs))
    assert isinstance(activity.outputs, type(subactivity.outputs))
    assert activity.outputs is not add_task.outputs
    assert activity.outputs is not subactivity.outputs
    assert activity.output_source is not add_task.outputs
    assert activity.output_source is subactivity.outputs

    # test single job and reference output
    add_task = get_task()
    subactivity = Activity(tasks=[add_task])
    activity = Activity(
        name="Add", tasks=[subactivity], outputs=subactivity.outputs.value
    )
    assert isinstance(activity.outputs, Value)

    # test multi job and list multi outputs
    add_task1 = get_task()
    subactivity1 = Activity(tasks=[add_task1])
    add_task2 = get_task()
    subactivity2 = Activity(tasks=[add_task2])
    activity = Activity(
        tasks=[subactivity1, subactivity2],
        outputs=[subactivity1.outputs, subactivity2.outputs.value],
    )
    assert isinstance(activity.outputs, Value)

    # dict outputs
    add_task = get_task()
    subactivity = Activity(tasks=[add_task])
    activity = Activity(
        tasks=[subactivity],
        outputs={"a": subactivity.outputs, "b": subactivity.outputs.value},
    )
    assert isinstance(activity.outputs, Dynamic)

    # test all jobflow included needed to generate outputs
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

    # test complex object containing job given rather than outputs
    with pytest.raises(ValueError):
        add_task = get_task()
        subactivity = Activity(tasks=[add_task])
        Activity(tasks=[subactivity], outputs={1: [[{"a": subactivity}]]})


def test_activity_sharing():
    from jobflow import Activity

    # test that jobflow cannot be shared between multiple jobflow
    add_task = get_task()
    shared_activity = Activity(tasks=[add_task])
    activity = Activity(tasks=[shared_activity])
    assert activity

    with pytest.raises(ValueError):
        Activity(tasks=[shared_activity])


def test_task_sharing():
    from jobflow import Activity

    # test that tasks cannot be shared between multiple jobflow
    shared_task = get_task()
    activity = Activity(tasks=[shared_task])
    assert activity

    with pytest.raises(ValueError):
        Activity(tasks=[shared_task])


def test_task_multiplicity():
    from jobflow import Activity

    # test that two of the same job cannot be used in the same activity
    add_task = get_task()
    with pytest.raises(ValueError):
        Activity(tasks=[add_task, add_task])


def test_activity_multiplicity():
    from jobflow import Activity

    # test that two of the same activity cannot be used in the same activity
    add_task = get_task()
    activity = Activity(tasks=[add_task])

    with pytest.raises(ValueError):
        Activity(tasks=[activity, activity])


def test_task_dag_validation():
    from jobflow import Activity, Task

    # test tasks out of order
    task1 = Task(function=("builtins", "sum"), args=(1, 2))
    task2 = Task(function=("builtins", "sum"), args=(task1.outputs.value, 2))
    activity = Activity(tasks=[task2, task1])
    with pytest.raises(ValueError):
        activity.validate()

    # test cycle detection of tasks
    task1 = Task(function=("builtins", "sum"), args=(1, 2))
    task2 = Task(function=("builtins", "sum"), args=(task1.outputs.value, 2))
    task1.function_args = (task2.outputs.value, 2)
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
    from jobflow import Activity, Task

    # test cycle detection of sub-jobflow
    task1 = Task(function=("builtins", "sum"), args=(1, 2))
    subactivity1 = Activity(tasks=[task1])

    task2 = Task(function=("builtins", "sum"), args=(subactivity1.outputs.value, 2))
    subactivity2 = Activity(tasks=[task2])
    activity = Activity(tasks=[subactivity1, subactivity2])
    with pytest.raises(ValueError):
        activity.validate()

    # other things to test:
    # - DAG for jobflow and tasks
    # - test all tasks (jobflow or tasks) included for graph to work
    # - test all the above bad inputs but for job Job object args and kwargs
    # - add validate call inside either task_to_wf or iterflow


def test_serialization():
    import json

    from monty.json import MontyDecoder, MontyEncoder

    from jobflow import Activity

    activity = Activity("MyActivity", [])
    activity_host = Activity("MyActivity", [activity])
    host_uuid = activity_host.uuid

    encoded_activity = json.loads(MontyEncoder().encode(activity_host))
    decoded_activity = MontyDecoder().process_decoded(encoded_activity)

    assert decoded_activity.tasks[0].host == host_uuid
