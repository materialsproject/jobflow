from uuid import uuid4

import pytest


def add(a, b=5):
    return a + b


def test_job_init():
    from jobflow.core.job import Job

    # test basic init
    test_job = Job(function=print, function_args=("I am a job",))
    assert test_job
    assert test_job.function == print
    assert test_job.name == "print"
    assert test_job.function_args == ("I am a job",)
    assert test_job.function_kwargs == {}
    assert test_job.uuid is not None
    assert test_job.output.uuid == test_job.uuid

    # test init with kwargs
    test_job = Job(function=add, function_args=(1,), function_kwargs={"b": 2})
    assert test_job
    assert test_job.function == add
    assert test_job.name == "add"
    assert test_job.function_args == (1,)
    assert test_job.function_kwargs == {"b": 2}
    assert test_job.uuid is not None
    assert test_job.uuid == test_job.output.value.uuid


def test_job_run(capsys, memory_jobstore):
    from jobflow.core.job import Job

    # test basic run
    test_job = Job(function=print, function_args=("I am a job",))
    response = test_job.run(memory_jobstore)
    assert capsys.readouterr().out == "I am a job\n"
    assert type(response).__name__ == "Response"

    # test run with outputs
    test_job = Job(function=add, function_args=(1,), function_kwargs={"b": 2})
    response = test_job.run(memory_jobstore)
    assert type(response).__name__ == "Response"
    assert response.output == 3

    # test run with input references
    test_job = Job(add, function_args=(test_job.output,))
    response = test_job.run(memory_jobstore)
    assert type(response).__name__ == "Response"
    assert response.output == 8


def test_job_input_references():
    from jobflow.core.job import Job
    from jobflow.core.outputs import Number
    from jobflow.core.reference import OutputReference

    ref = OutputReference(uuid4(), "b")
    test_job = Job(
        function=(__name__, "add"), args=(1,), kwargs={"b": ref}, outputs=Number
    )
    references = test_job.input_references

    assert set(references) == {ref}


def test_job_output_references():
    from jobflow.core.job import Job
    from jobflow.core.outputs import Number

    test_job = Job(
        function=(__name__, "add"), args=(1,), kwargs={"b": 2}, outputs=Number
    )
    references = test_job.output_references

    assert set(references) == {test_job.outputs.value}


def test_job_resolve_args(output_store):
    from jobflow.core.job import Job
    from jobflow.core.reference import OutputReference

    # test basic run with no references
    test_job = Job(function=("builtins", "print"), args=("I am a job",))
    resolved_job = test_job.resolve_args()
    assert test_job == resolved_job

    ref = OutputReference(uuid4(), "b")
    cache = {ref.uuid: {ref.name: 2}}

    # test run with input references
    test_job = Job(function=(__name__, "add"), args=(1,), kwargs={"b": ref})
    resolved_job = test_job.resolve_args(output_cache=cache)
    assert test_job == resolved_job
    assert resolved_job.function_kwargs["b"] == 2

    # test resolve with inplace=False
    test_job = Job(function=(__name__, "add"), args=(1,), kwargs={"b": ref})
    resolved_job = test_job.resolve_args(output_cache=cache, inplace=False)
    assert test_job != resolved_job
    assert resolved_job.function_kwargs["b"] == 2
    assert isinstance(test_job.function_kwargs["b"], OutputReference)

    # test resolve with allow errors
    test_job = Job(function=(__name__, "add"), args=(1,), kwargs={"b": ref})
    resolved_job = test_job.resolve_args(output_cache={}, error_on_missing=False)
    assert test_job == resolved_job
    assert resolved_job.function_kwargs["b"] == ref

    # test resolve with store
    output_store.update({"uuid": str(ref.uuid), ref.name: 2}, key="uuid")
    test_job = Job(function=(__name__, "add"), args=(1,), kwargs={"b": ref})
    resolved_job = test_job.resolve_args(output_store=output_store)
    assert resolved_job.function_kwargs["b"] == 2

    # test cache is preferred over store
    output_store.update({"uuid": str(ref.uuid), ref.name: 10}, key="uuid")
    test_job = Job(function=(__name__, "add"), args=(1,), kwargs={"b": ref})
    resolved_job = test_job.resolve_args(output_store=output_store, output_cache=cache)
    assert resolved_job.function_kwargs["b"] == 2


def test_job_decorator():
    from jobflow.core.job import job
    from jobflow.core.outputs import Dynamic, Number

    # test basic init
    decorated = job(print)
    test_job = decorated("I am a job")
    assert test_job
    assert test_job.function == ("builtins", "print")
    assert test_job.function_args == ("I am a job",)
    assert test_job.function_kwargs == {}
    assert test_job.uuid is not None
    assert isinstance(test_job.outputs, Dynamic)

    # test init with outputs
    decorated = job(add, outputs=Number)
    test_job = decorated(1, b=2)
    assert test_job
    assert test_job.function == (__name__, "add")
    assert test_job.function_args == (1,)
    assert test_job.function_kwargs == {"b": 2}
    assert test_job.uuid is not None
    assert isinstance(test_job.outputs, Number)
    assert test_job.uuid == test_job.outputs.value.uuid

    # test applying the decorator without arguments
    @job
    def print_message(message):
        print(message)

    test_job = print_message("I am a job")
    assert test_job
    assert test_job.function == (__name__, "print_message")
    assert test_job.args == ("I am a job",)
    assert test_job.kwargs == {}
    assert test_job.uuid is not None
    assert isinstance(test_job.outputs, Dynamic)

    # test applying the decorator with arguments
    @job(outputs=Number)
    def add_numbers(a, b=10):
        return Number(a + b)

    test_job = add_numbers(1, b=2)
    assert test_job
    assert test_job.function == (__name__, "add_numbers")
    assert test_job.function_args == (1,)
    assert test_job.function_kwargs == {"b": 2}
    assert test_job.uuid is not None
    assert isinstance(test_job.outputs, Number)
    assert test_job.uuid == test_job.outputs.value.uuid

    # test setting outputs to None
    @job(outputs=None)
    def add_numbers(a, b=10):
        print(a + b)

    test_job = add_numbers(1, b=2)
    assert test_job.outputs is None


def test_job_response():
    # no need to test init as it is just a dataclass, instead test from_job_returns
    # test no job returns
    from jobflow.core.activity import Activity
    from jobflow.core.job import Detour, JobResponse, Stop, Store
    from jobflow.core.outputs import Dynamic, Number, Value

    response = JobResponse.from_job_returns(None)
    assert response == JobResponse()

    # test single output
    response = JobResponse.from_job_returns(1)
    assert isinstance(response.outputs, Value)
    assert response.outputs.value == 1

    # test list output
    response = JobResponse.from_job_returns([1, 2, 3])
    assert isinstance(response.outputs, Value)
    assert response.outputs.value == [1, 2, 3]

    # test tuple output
    response = JobResponse.from_job_returns((1, 2, 3))
    assert isinstance(response.outputs, Value)
    assert response.outputs.value == (1, 2, 3)

    # test dict output
    response = JobResponse.from_job_returns({"a": 1, "b": 2})
    assert isinstance(response.outputs, Dynamic)
    assert response.outputs.a == 1
    assert response.outputs.b == 2

    # test outputs
    outputs = Number(5)
    response = JobResponse.from_job_returns(outputs)
    assert response == JobResponse(outputs=outputs)

    # test detour
    detour = Detour(Activity())
    response = JobResponse.from_job_returns(detour)
    assert response == JobResponse(detour=detour.activity)

    # test store
    store = Store({"my_data": 123})
    response = JobResponse.from_job_returns(store)
    assert response == JobResponse(store=store.data)

    # test stop
    stop = Stop(stop_jobs=True, stop_children=True, stop_activities=True)
    response = JobResponse.from_job_returns(stop)
    assert response == JobResponse(
        stop_jobs=True, stop_activities=True, stop_children=True
    )

    # test multiple
    response = JobResponse.from_job_returns((outputs, store, stop))
    assert response == JobResponse(
        outputs=outputs,
        store=store.data,
        stop_jobs=True,
        stop_activities=True,
        stop_children=True,
    )

    # test multiple with no outputs class
    response = JobResponse.from_job_returns((123, store, stop))
    assert response == JobResponse(
        outputs=Value(123),
        store=store.data,
        stop_jobs=True,
        stop_activities=True,
        stop_children=True,
    )

    # test detour overrides outputs
    response = JobResponse.from_job_returns((outputs, detour, store, stop))
    assert response == JobResponse(
        outputs=None,
        detour=detour.activity,
        store=store.data,
        stop_jobs=True,
        stop_activities=True,
        stop_children=True,
    )

    # test malformed outputs
    with pytest.raises(ValueError):
        JobResponse.from_job_returns([1, 2, 3, store])

    # test multiple of the same outputs
    with pytest.raises(ValueError):
        JobResponse.from_job_returns((store, store))

    with pytest.raises(ValueError):
        JobResponse.from_job_returns((detour, detour))


#
# # print("calling method")
# a = maker.make(1, 2)
#
#
# class Test:
#
#     @job
#     @staticmethod
#     def static_before(a, b):
#         return a + b
#
#     @staticmethod
#     @job
#     def static_after(a, b):
#         return a + b
#
#     @job
#     @classmethod
#     def class_before(cls, a, b):
#         return a + b
#
#     @classmethod
#     @job
#     def class_after(cls, a, b):
#         return a + b
#
#
# # print("calling static after")
# a = Test.static_after(3, 4)
# # print("calling static before")
# a = Test.static_before(3, 4)
# #
# # print("calling class after")
# a = Test.class_after(3, 4)
# # print("calling class before")
# a = Test.class_before(3, 4)
#
# output = run_locally(a)
# print(list(output.values())[0])
#
