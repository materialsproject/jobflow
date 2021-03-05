from activities.core.task import Task
from activities.core.outputs import Number
from activities.core.reference import Reference
from uuid import uuid4


def add(a, b=5):
    return Number(a + b)


def test_task_init():
    # test basic init
    test_task = Task(function=("builtins", "print"), args=("I am a task",))
    assert test_task
    assert test_task.function == ("builtins", "print")
    assert test_task.args == ("I am a task", )
    assert test_task.kwargs == {}
    assert test_task.uuid is not None
    assert test_task.outputs is None

    # test init with outputs
    test_task = Task(function=(__name__, "add"), args=(1, ), kwargs={"b": 2}, outputs=Number)
    assert test_task
    assert test_task.function == (__name__, "add")
    assert test_task.args == (1, )
    assert test_task.kwargs == {"b": 2}
    assert test_task.uuid is not None
    assert isinstance(test_task.outputs, Number)
    assert test_task.uuid == test_task.outputs.number.uuid


def test_task_run(capsys):
    # test basic run
    test_task = Task(function=("builtins", "print"), args=("I am a task",))
    response = test_task.run()
    assert capsys.readouterr().out == "I am a task\n"
    assert type(response).__name__ == "TaskResponse"

    # test run with outputs
    test_task = Task(function=(__name__, "add"), args=(1, ), kwargs={"b": 2}, outputs=Number)
    response = test_task.run()
    assert type(response).__name__ == "TaskResponse"
    assert isinstance(response.outputs, Number)
    assert response.outputs.number == 3

    # test run with input references
    ref = Reference(uuid4(), "b")
    test_task = Task(function=(__name__, "add"), args=(1, ), kwargs={"b": ref}, outputs=Number)
    response = test_task.run(output_cache={ref.uuid: {ref.name: 2}})
    assert type(response).__name__ == "TaskResponse"
    assert isinstance(response.outputs, Number)
    assert response.outputs.number == 3


def test_task_input_references():
    ref = Reference(uuid4(), "b")
    test_task = Task(
        function=(__name__, "add"), args=(1, ), kwargs={"b": ref}, outputs=Number
    )
    references = test_task.input_references

    assert set(references) == {ref}


def test_task_output_references():
    from activities.core.task import Task
    from activities.core.outputs import Number

    test_task = Task(function=(__name__, "add"), args=(1, ), kwargs={"b": 2}, outputs=Number)
    references = test_task.output_references

    assert set(references) == {test_task.outputs.number}


def test_task_resolve_args():
    # test basic run with no references
    test_task = Task(function=("builtins", "print"), args=("I am a task",))
    resolved_task = test_task.resolve_args()
    assert test_task == resolved_task

    ref = Reference(uuid4(), "b")
    cache = {ref.uuid: {ref.name: 2}}

    # test run with input references
    test_task = Task(function=(__name__, "add"), args=(1, ), kwargs={"b": ref})
    resolved_task = test_task.resolve_args(output_cache=cache)
    assert test_task == resolved_task
    assert resolved_task.kwargs["b"] == 2

    # test resolve with inplace=False
    test_task = Task(function=(__name__, "add"), args=(1, ), kwargs={"b": ref})
    resolved_task = test_task.resolve_args(output_cache=cache, inplace=False)
    assert test_task != resolved_task
    assert resolved_task.kwargs["b"] == 2
    assert isinstance(test_task.kwargs["b"], Reference)

    # test resolve with allow errors
    test_task = Task(function=(__name__, "add"), args=(1, ), kwargs={"b": ref})
    resolved_task = test_task.resolve_args(output_cache={}, error_on_missing=False)
    assert test_task == resolved_task
    assert resolved_task.kwargs["b"] == ref

    # test resolve with store
    from maggma.stores import MemoryStore

    store = MemoryStore()
    store.connect()
    store.update({"uuid": str(ref.uuid), ref.name: 2}, key="uuid")

    test_task = Task(function=(__name__, "add"), args=(1, ), kwargs={"b": ref})
    resolved_task = test_task.resolve_args(output_store=store)
    assert resolved_task.kwargs["b"] == 2

    # test cache is preferred over store
    store.update({"uuid": str(ref.uuid), ref.name: 10}, key="uuid")
    test_task = Task(function=(__name__, "add"), args=(1, ), kwargs={"b": ref})
    resolved_task = test_task.resolve_args(output_store=store, output_cache=cache)
    assert resolved_task.kwargs["b"] == 2
