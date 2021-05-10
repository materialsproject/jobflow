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
    test_job = Job(print, function_args=("I am a job",))
    response = test_job.run(memory_jobstore)
    assert capsys.readouterr().out == "I am a job\n"
    assert type(response).__name__ == "Response"

    # test run with outputs
    test_job = Job(add, function_args=(1,), function_kwargs={"b": 2})
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
    from jobflow.core.reference import OutputReference

    ref1 = OutputReference("12345")
    ref2 = OutputReference("54321")
    test_job = Job(add, function_args=(ref1,), function_kwargs={"b": ref2})
    assert set(test_job.input_references) == {ref1, ref2}
    assert set(test_job.input_uuids) == {"12345", "54321"}
    assert test_job.input_references_grouped == {"12345": (ref1,), "54321": (ref2,)}

    # test using reference attributes
    ref1 = OutputReference("12345", attributes=("name",))
    ref2 = OutputReference("12345", attributes=("value",))
    test_job = Job(add, function_args=(ref1,), function_kwargs={"b": ref2})
    assert set(test_job.input_references) == {ref1, ref2}
    assert set(test_job.input_uuids) == {"12345"}
    assert test_job.input_references_grouped == {"12345": (ref1, ref2)}


def test_job_resolve_args(memory_jobstore):
    from jobflow.core.job import Job
    from jobflow.core.reference import OnMissing, OutputReference

    # test basic run with no references
    test_job = Job(print, function_args=("I am a job",))
    resolved_job = test_job.resolve_args(memory_jobstore)
    assert test_job == resolved_job

    ref = OutputReference("1234")
    memory_jobstore.update({"uuid": "1234", "index": 1, "output": 2})

    # test run with input references
    test_job = Job(add, function_args=(1,), function_kwargs={"b": ref})
    resolved_job = test_job.resolve_args(memory_jobstore)
    assert test_job == resolved_job
    assert resolved_job.function_kwargs["b"] == 2

    # test resolve with inplace=False
    test_job = Job(add, function_args=(1,), function_kwargs={"b": ref})
    resolved_job = test_job.resolve_args(memory_jobstore, inplace=False)
    assert test_job != resolved_job
    assert resolved_job.function_kwargs["b"] == 2
    assert isinstance(test_job.function_kwargs["b"], OutputReference)

    # test resolve with on missing == error
    ref = OutputReference("a")
    test_job = Job(add, function_args=(1,), function_kwargs={"b": ref})
    with pytest.raises(ValueError):
        test_job.resolve_args(memory_jobstore, on_missing=OnMissing.ERROR)

    # test resolve with on missing == none
    resolved_job = test_job.resolve_args(memory_jobstore, on_missing=OnMissing.NONE)
    assert test_job == resolved_job

    # test resolve with on missing == pass
    test_job = Job(add, function_args=(1,), function_kwargs={"b": ref})
    resolved_job = test_job.resolve_args(memory_jobstore, on_missing=OnMissing.PASS)
    assert test_job == resolved_job
    assert resolved_job.function_kwargs["b"] == ref


def test_job_decorator():
    from jobflow.core.job import job

    # test basic init
    decorated = job(print)
    test_job = decorated("I am a job")
    assert test_job
    assert test_job.function == print
    assert test_job.function_args == ("I am a job",)
    assert test_job.function_kwargs == {}
    assert test_job.name == "print"
    assert type(test_job.output).__name__ == "OutputReference"
    assert test_job.uuid is not None
    assert test_job.uuid == test_job.output.uuid

    # test init with outputs
    decorated = job(add)
    test_job = decorated(1, b=2)
    assert test_job
    assert test_job.function == add
    assert test_job.function_args == (1,)
    assert test_job.function_kwargs == {"b": 2}
    assert test_job.name == "add"
    assert test_job.uuid is not None
    assert type(test_job.output).__name__ == "OutputReference"
    assert test_job.uuid == test_job.output.uuid

    # test applying the decorator without arguments
    @job
    def print_message(message):
        print(message)

    test_job = print_message("I am a job")
    assert test_job
    assert test_job.function.__name__ == print_message.__name__
    assert test_job.function.__module__ == print_message.__module__
    assert test_job.function_args == ("I am a job",)
    assert test_job.function_kwargs == {}
    assert test_job.uuid is not None
    assert type(test_job.output).__name__ == "OutputReference"
    assert test_job.uuid == test_job.output.uuid

    # test applying the decorator with arguments
    @job
    def add_numbers(a, b=10):
        return a + b

    test_job = add_numbers(1, b=2)
    assert test_job
    assert test_job.function.__name__ == add_numbers.__name__
    assert test_job.function.__module__ == add_numbers.__module__
    assert test_job.function_args == (1,)
    assert test_job.function_kwargs == {"b": 2}
    assert test_job.uuid is not None
    assert type(test_job.output).__name__ == "OutputReference"
    assert test_job.uuid == test_job.output.uuid


def test_response():
    # no need to test init as it is just a dataclass, instead test from_job_returns
    # test no job returns
    from jobflow import Schema
    from jobflow.core.job import Response

    response = Response.from_job_returns(None)
    assert response == Response()

    # test single output
    response = Response.from_job_returns(1)
    assert response.output == 1

    # test list output
    response = Response.from_job_returns([1, 2, 3])
    assert response.output == [1, 2, 3]

    # test tuple output
    response = Response.from_job_returns((1, 2, 3))
    assert response.output == (1, 2, 3)

    # test dict output
    response = Response.from_job_returns({"a": 1, "b": 2})
    assert response.output == {"a": 1, "b": 2}

    # test Response output
    response_original = Response(output=(1, 2, 3), stop_children=True)
    response_processed = Response.from_job_returns(response_original)
    assert response_original == response_processed

    # test Response and another output
    with pytest.raises(ValueError):
        Response.from_job_returns([response_original, 5])

    # test schema
    class MySchema(Schema):
        number: int
        name: str

    response = Response.from_job_returns(
        {"number": "5", "name": "Ian"}, output_schema=MySchema
    )
    assert response.output.__class__.__name__ == "MySchema"
    assert response.output.number == 5
    assert response.output.name == "Ian"

    # test schema does not match output
    from pydantic import ValidationError

    with pytest.raises(ValidationError):
        Response.from_job_returns({"number": "5"}, output_schema=MySchema)


def test_serialization():
    import json

    from monty.json import MontyDecoder, MontyEncoder

    from jobflow import Job

    test_job = Job(function=add, function_args=(1,), function_kwargs={"b": 2})

    uuid = test_job.uuid

    encoded_job = json.loads(MontyEncoder().encode(test_job))
    decoded_job = MontyDecoder().process_decoded(encoded_job)

    assert decoded_job.uuid == uuid


def test_class_jobs(memory_jobstore):
    from jobflow.core.job import job

    class Test:
        @job
        @staticmethod
        def static_before(x, y):
            return x + y

        @staticmethod
        @job
        def static_after(x, y):
            return x + y

        @job
        @classmethod
        def class_before(cls, x, y):
            return x + y

        @classmethod
        @job
        def class_after(cls, x, y):
            return x + y

        @job
        def instance(self, x, y):
            return x + y

    # test staticmethod decorator put after job decorator
    test_job = Test.static_after(3, 4)
    response = test_job.run(memory_jobstore)
    assert response.output == 7

    # test staticmethod decorator put before job decorator
    test_job = Test.static_before(3, 4)
    response = test_job.run(memory_jobstore)
    assert response.output == 7

    # test classmethod decorator put after job decorator
    test_job = Test.class_after(3, 4)
    response = test_job.run(memory_jobstore)
    assert response.output == 7

    # test classmethod decorator put before job decorator
    test_job = Test.class_before(3, 4)
    response = test_job.run(memory_jobstore)
    assert response.output == 7

    # test job decorator in instance method
    test_job = Test().instance(3, 4)
    response = test_job.run(memory_jobstore)
    assert response.output == 7
