import pytest


def add(a, b=5):
    return a + b


def bad_output():
    return {1, 2, 3}


def test_job_init():
    from jobflow.core.job import Job

    # test basic init
    test_job = Job(function=add, function_args=("I am a job",))
    assert test_job
    assert test_job.function == add
    assert test_job.name == "add"
    assert test_job.function_args == ("I am a job",)
    assert test_job.function_kwargs == {}
    assert test_job.uuid is not None
    assert test_job.output.uuid == test_job.uuid

    # test init no args
    test_job = Job(function=add, function_args=())
    assert test_job
    assert test_job.function == add
    assert test_job.name == "add"
    assert test_job.function_args == ()
    assert test_job.function_kwargs == {}
    assert test_job.uuid is not None
    assert test_job.output.uuid == test_job.uuid

    # test job as another job as input
    with pytest.warns(UserWarning):
        Job(function=add, function_args=(test_job,))

    # test init with kwargs
    test_job = Job(function=add, function_args=(1,), function_kwargs={"b": 2})
    assert test_job
    assert test_job.function == add
    assert test_job.name == "add"
    assert test_job.function_args == (1,)
    assert test_job.function_kwargs == {"b": 2}
    assert test_job.uuid is not None
    assert test_job.uuid == test_job.output.value.uuid

    # test init with stores
    test_job = Job(
        function=add,
        function_args=(1,),
        function_kwargs={"b": 2},
        data="output",
        graphs="graph",
    )
    assert test_job
    assert test_job.uuid == test_job.output.value.uuid
    assert test_job._kwargs == {"data": "output", "graphs": "graph"}

    # check giving True for multiple stores fails
    with pytest.raises(
        ValueError, match="Cannot select True for multiple additional stores"
    ):
        Job(function=add, function_args=(1,), data=True, graphs=True)

    # test changing job name (test needed due to setattr override
    test_job = Job(function=add, function_args=("I am a job",))
    test_job.name = "abc"
    assert test_job.name == "abc"

    test_job.name += "xyz"
    assert test_job.name == "abcxyz"


def test_job_run(capsys, memory_jobstore, memory_data_jobstore):
    from jobflow.core.job import Job, Response

    # test basic run
    test_job = Job(print, function_args=("I am a job",))
    response = test_job.run(memory_jobstore)
    assert capsys.readouterr().out == "I am a job\n"
    assert isinstance(response, Response)

    # test run with outputs
    test_job = Job(add, function_args=(1,), function_kwargs={"b": 2})
    response = test_job.run(memory_jobstore)
    assert isinstance(response, Response)
    assert response.output == 3

    # test run with input references
    test_job = Job(add, function_args=(test_job.output,))
    response = test_job.run(memory_jobstore)
    assert isinstance(response, Response)
    assert response.output == 8

    def add_response(a, b):
        return Response(output=a + b, stop_children=True)

    test_job = Job(add_response, function_args=(1, 2))
    response = test_job.run(memory_jobstore)
    assert isinstance(response, Response)
    assert response.output == 3
    assert response.stop_children

    # test run with outputs and data store
    test_job = Job(add, function_args=(1,), function_kwargs={"b": 2}, data=True)
    response = test_job.run(memory_data_jobstore)
    assert isinstance(response, Response)
    assert response.output == 3

    # check output was not stored in the docs store
    result = memory_data_jobstore.query_one({"uuid": test_job.uuid})
    assert isinstance(result["output"], dict)
    assert "blob_uuid" in result["output"]

    # check the output can be resolved
    result = memory_data_jobstore.query_one({"uuid": test_job.uuid}, load=True)
    assert result["output"] == 3

    # test non MSONable output
    test_job = Job(bad_output)
    with pytest.raises(RuntimeError):
        test_job.run(memory_jobstore)

    # test on standard library functions
    import time

    test_job = Job(time.sleep, function_args=(0.001,))
    response = test_job.run(memory_jobstore)
    assert isinstance(response, Response)


def test_replace_response(memory_jobstore):
    from jobflow import Flow, Job, Response

    def replace_job():
        job = Job(add, function_args=(1,))
        return Response(replace=job)

    def replace_list_job():
        job1 = Job(add, function_args=(1,))
        job2 = Job(add, function_args=(job1.output,))
        return Response(replace=[job1, job2])

    def replace_flow():
        job = Job(add, function_args=(1,))
        flow = Flow([job], output=job.output)
        return Response(replace=flow)

    def replace_flow_multioutput():
        job1 = Job(add, function_args=(1,))
        job2 = Job(add, function_args=(job1.output,))
        flow = Flow([job1, job2], output={"1": job1.output, "2": job2.output})
        return Response(replace=flow)

    def replace_list_flow():
        job1 = Job(add, function_args=(1,))
        job2 = Job(add, function_args=(job1.output,))
        job3 = Job(add, function_args=(5,))
        job4 = Job(add, function_args=(job3.output,))
        flow1 = Flow([job1, job2], output={"1": job1.output, "2": job2.output})
        flow2 = Flow([job3, job4], output={"3": job3.output, "4": job4.output})
        return Response(replace=[flow1, flow2])

    # replace with job
    metadata = {"hi": "I exist"}
    test_job = Job(replace_job, metadata=metadata, output_schema="123")
    response = test_job.run(memory_jobstore)
    assert isinstance(response.replace, Flow)
    assert len(response.replace) == 1
    assert response.replace[0].index == 2
    assert response.replace[0].uuid == test_job.uuid
    assert response.replace[0].metadata == metadata
    assert response.replace[0].output_schema == "123"
    assert test_job.hosts == []
    assert response.replace.hosts == []
    assert response.replace[0].hosts == [response.replace.uuid]

    # replace with list of job
    test_job = Job(replace_list_job, metadata=metadata, output_schema="123")
    response = test_job.run(memory_jobstore)
    assert isinstance(response.replace, Flow)
    assert response.replace[-1].function == add
    assert len(response.replace) == 2
    # currently output schema and metadata ignored in this case
    for j in response.replace:
        assert j.hosts == [response.replace.uuid]

    # replace with flow with outputs
    test_job = Job(replace_flow, metadata=metadata, output_schema="123", data=True)
    # wrap the job in a Flow to check hosts
    test_flow = Flow([test_job])
    response = test_job.run(memory_jobstore)
    assert isinstance(response.replace, Flow)
    assert response.replace[-1].index == 2
    assert response.replace[-1].uuid == test_job.uuid
    assert response.replace[-1].metadata == metadata
    assert response.replace[-1].output_schema == "123"
    assert response.replace[-1]._kwargs["data"]
    assert response.replace.output is not None
    for j in response.replace:
        assert j.hosts == [response.replace.uuid, test_flow.uuid]

    # replace with flow with multi outputs
    test_job = Job(replace_flow_multioutput, metadata=metadata, output_schema="123")
    response = test_job.run(memory_jobstore)
    assert isinstance(response.replace, Flow)
    assert response.replace[-1].index == 2
    assert response.replace[-1].uuid == test_job.uuid
    assert response.replace[-1].metadata == metadata
    assert response.replace[-1].output_schema == "123"
    assert response.replace.output is not None

    # replace with list of flow
    test_job = Job(replace_list_flow, metadata=metadata, output_schema="123")
    response = test_job.run(memory_jobstore)
    assert isinstance(response.replace, Flow)
    assert isinstance(response.replace[-1], Flow)
    assert len(response.replace) == 2
    for f in response.replace:
        for j in f:
            assert j.hosts == [f.uuid, response.replace.uuid]
    # currently output schema and metadata ignored in this case


def test_job_config(memory_jobstore):
    from jobflow import (
        CURRENT_JOB,
        Job,
        JobConfig,
        OnMissing,
        OutputReference,
        Response,
    )

    def store_exposed():
        return CURRENT_JOB.store is not None

    def reference_resolved(arg):
        return not isinstance(arg, OutputReference)

    def return_arg(arg):
        return arg

    # test expose store
    config = JobConfig(expose_store=False)
    test_job = Job(store_exposed, config=config)
    response = test_job.run(memory_jobstore)
    assert response.output is False

    config = JobConfig(expose_store=True)
    test_job = Job(store_exposed, config=config)
    response = test_job.run(memory_jobstore)
    assert response.output is True

    ref = OutputReference("1234")
    memory_jobstore.update({"uuid": "1234", "index": 0, "output": 5})
    config = JobConfig(resolve_references=False)
    test_job = Job(reference_resolved, function_args=(ref,), config=config)
    response = test_job.run(memory_jobstore)
    assert response.output is False

    config = JobConfig(resolve_references=True)
    test_job = Job(reference_resolved, function_args=(ref,), config=config)
    response = test_job.run(memory_jobstore)
    assert response.output is True

    ref = OutputReference("xyz")
    config = JobConfig(on_missing_references=OnMissing.ERROR)
    test_job = Job(return_arg, function_args=(ref,), config=config)
    with pytest.raises(
        ValueError, match="Could not resolve reference - xyz not in store"
    ):
        test_job.run(memory_jobstore)

    config = JobConfig(on_missing_references=OnMissing.NONE)
    test_job = Job(return_arg, function_args=(ref,), config=config)
    response = test_job.run(memory_jobstore)
    assert response.output is None

    config = JobConfig(on_missing_references=OnMissing.PASS)
    test_job = Job(return_arg, function_args=(ref,), config=config)
    response = test_job.run(memory_jobstore)
    assert isinstance(response.output, OutputReference)

    # test pass manager config
    def replace_job():
        job = Job(add, function_args=(1,))
        return Response(replace=job)

    def replace_list_job():
        job1 = Job(add, function_args=(1,))
        job2 = Job(add, function_args=(job1.output,))
        return Response(replace=[job1, job2])

    def replace_flow():
        from jobflow import Flow

        job = Job(add, function_args=(1,))
        flow = Flow([job], output=job.output)
        return Response(replace=flow)

    def addition_job():
        job = Job(add, function_args=(1,))
        return Response(addition=job)

    def detour_job():
        job = Job(add, function_args=(1,))
        return Response(detour=job)

    manager_config = {"abc": 1}
    manager_config2 = {"abc": 2}
    pass_config = JobConfig(manager_config=manager_config, pass_manager_config=True)
    nopass_config = JobConfig(manager_config=manager_config, pass_manager_config=False)
    response_config = JobConfig(
        manager_config=manager_config, response_manager_config=manager_config2
    )

    # test replace
    test_job = Job(replace_job, config=nopass_config)
    response = test_job.run(memory_jobstore)
    assert len(response.replace) == 1
    assert response.replace[0].config.manager_config == {}

    test_job = Job(replace_job, config=pass_config)
    response = test_job.run(memory_jobstore)
    assert response.replace[0].config.manager_config == manager_config

    test_job = Job(replace_job, config=response_config)
    response = test_job.run(memory_jobstore)
    assert response.replace[0].config.manager_config == manager_config2

    # test replace list of jobs
    test_job = Job(replace_list_job, config=nopass_config)
    response = test_job.run(memory_jobstore)
    for j in response.replace:
        assert j.config.manager_config == {}

    test_job = Job(replace_list_job, config=pass_config)
    response = test_job.run(memory_jobstore)
    for j in response.replace:
        assert j.config.manager_config == manager_config

    test_job = Job(replace_list_job, config=response_config)
    response = test_job.run(memory_jobstore)
    for j in response.replace:
        assert j.config.manager_config == manager_config2

    # test replace with flow
    test_job = Job(replace_flow, config=nopass_config)
    response = test_job.run(memory_jobstore)
    for j in response.replace:
        assert j.config.manager_config == {}

    test_job = Job(replace_flow, config=pass_config)
    response = test_job.run(memory_jobstore)
    for j in response.replace:
        assert j.config.manager_config == manager_config

    test_job = Job(replace_flow, config=response_config)
    response = test_job.run(memory_jobstore)
    for j in response.replace:
        assert j.config.manager_config == manager_config2

    # test addition
    test_job = Job(addition_job, config=nopass_config)
    response = test_job.run(memory_jobstore)
    assert len(response.addition) == 1
    assert response.addition[0].config.manager_config == {}

    test_job = Job(addition_job, config=pass_config)
    response = test_job.run(memory_jobstore)
    assert len(response.addition) == 1
    assert response.addition[0].config.manager_config == manager_config
    assert response.addition[0].hosts == [response.addition.uuid]

    test_job = Job(addition_job, config=response_config)
    response = test_job.run(memory_jobstore)
    assert response.addition[0].config.manager_config == manager_config2

    # test detour
    test_job = Job(detour_job, config=nopass_config)
    response = test_job.run(memory_jobstore)
    assert len(response.detour) == 1
    assert response.detour[0].config.manager_config == {}

    test_job = Job(detour_job, config=pass_config)
    response = test_job.run(memory_jobstore)
    assert len(response.detour) == 1
    assert response.detour[0].config.manager_config == manager_config
    assert response.detour[0].hosts == [response.detour.uuid]

    test_job = Job(detour_job, config=response_config)
    response = test_job.run(memory_jobstore)
    assert response.detour[0].config.manager_config == manager_config2


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
    ref1 = OutputReference("12345", attributes=(("i", "name"),))
    ref2 = OutputReference("12345", attributes=(("i", "value"),))
    test_job = Job(add, function_args=(ref1,), function_kwargs={"b": ref2})
    assert set(test_job.input_references) == {ref1, ref2}
    assert set(test_job.input_uuids) == {"12345"}
    assert set(test_job.input_references_grouped) == {"12345"}
    assert set(test_job.input_references_grouped["12345"]) == {ref1, ref2}


def test_job_resolve_args(memory_jobstore):
    from jobflow.core.job import Job
    from jobflow.core.reference import OutputReference

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

    # test init no args
    decorated = job(print)
    test_job = decorated()
    assert test_job
    assert test_job.function == print
    assert test_job.function_args == ()
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
    from pydantic import BaseModel

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
    with pytest.raises(
        ValueError,
        match="Response cannot be returned in combination with other outputs",
    ):
        Response.from_job_returns([response_original, 5])

    # test schema
    class MySchema(BaseModel):
        number: int
        name: str

    response = Response.from_job_returns(
        {"number": "5", "name": "Ian"}, output_schema=MySchema
    )
    assert type(response.output).__name__ == "MySchema"
    assert response.output.number == 5
    assert response.output.name == "Ian"

    # test schema does not match output
    from pydantic import ValidationError

    with pytest.raises(ValidationError):
        Response.from_job_returns({"number": "5"}, output_schema=MySchema)


def test_serialization():
    import json
    from datetime import datetime

    from monty.json import MontyDecoder, MontyEncoder

    from jobflow import Job

    test_job = Job(
        function=add, function_args=(1, datetime.now()), function_kwargs={"b": 2}
    )

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


def test_maker():
    from dataclasses import dataclass

    from jobflow import Maker, job

    @dataclass
    class AddMaker(Maker):
        name: str = "add"
        b: int = 2

        @job
        def make(self, a):
            return a + self.b

    maker = AddMaker()
    test_job = maker.make(2)
    assert test_job.maker == maker

    # test setting job name; should also update the maker name
    test_job.name = "abc"
    assert test_job.name == "abc"
    assert test_job.maker.name == "abc"

    test_job.name += "xyz"
    assert test_job.name == "abcxyz"
    assert test_job.maker.name == "abcxyz"


def test_graph():
    from jobflow import Job

    # test no inputs
    test_job = Job(add, function_args=(1,), function_kwargs={"b": 2})
    graph = test_job.graph
    assert len(graph.nodes) == 1
    assert len(graph.edges) == 0

    # test arg inputs
    test_job1 = Job(add, function_args=(1,), function_kwargs={"b": 2})
    test_job = Job(add, function_args=(test_job1.output,), function_kwargs={"b": 2})
    graph = test_job.graph
    assert len(graph.nodes) == 2
    assert len(graph.edges) == 1

    # test arg input properties
    test_job1 = Job(add, function_args=(1,), function_kwargs={"b": 2})
    test_job = Job(
        add, function_args=(test_job1.output.value,), function_kwargs={"b": 2}
    )
    graph = test_job.graph
    assert len(graph.nodes) == 2
    assert len(graph.edges) == 1
    assert graph.get_edge_data(test_job1.uuid, test_job.uuid)["properties"] == "value"

    # test arg and kwargs inputs
    test_job1 = Job(add, function_args=(1,), function_kwargs={"b": 2})
    test_job2 = Job(add, function_args=(1,), function_kwargs={"b": 2})
    test_job = Job(
        add, function_args=(test_job1.output,), function_kwargs={"b": test_job2.output}
    )
    graph = test_job.graph
    assert len(graph.nodes) == 3
    assert len(graph.edges) == 2

    # test arg and kwargs input properties
    test_job1 = Job(add, function_args=(1,), function_kwargs={"b": 2})
    test_job = Job(
        add,
        function_args=(test_job1.output.value,),
        function_kwargs={"b": test_job1.output.name},
    )
    graph = test_job.graph
    assert len(graph.nodes) == 2
    assert len(graph.edges) == 1
    assert set(graph.get_edge_data(test_job1.uuid, test_job.uuid)["properties"]) == {
        "value",
        "name",
    }


def test_set_uuid():
    from jobflow import Job

    test_job = Job(add, function_args=(1,))
    test_job.set_uuid("1234")
    assert test_job.uuid == "1234"
    assert test_job.output.uuid == "1234"


def test_update_kwargs():
    from jobflow import Job

    # test no filter
    test_job = Job(add, function_args=(1,), function_kwargs={"b": 2})
    test_job.update_kwargs({"b": 5})
    assert test_job.function_kwargs["b"] == 5

    # test name filter
    test_job = Job(add, function_args=(1,), function_kwargs={"b": 2})
    test_job.update_kwargs({"b": 5}, name_filter="add")
    assert test_job.function_kwargs["b"] == 5

    test_job = Job(add, function_args=(1,), function_kwargs={"b": 2})
    test_job.update_kwargs({"b": 5}, name_filter="div")
    assert test_job.function_kwargs["b"] == 2

    # test function filter
    test_job = Job(add, function_args=(1,), function_kwargs={"b": 2})
    test_job.update_kwargs({"b": 5}, function_filter=add)
    assert test_job.function_kwargs["b"] == 5

    test_job = Job(add, function_args=(1,), function_kwargs={"b": 2})
    test_job.update_kwargs({"b": 5}, function_filter=list)
    assert test_job.function_kwargs["b"] == 2

    # test dict mod
    test_job = Job(add, function_args=(1,), function_kwargs={"b": 2})
    test_job.update_kwargs({"_inc": {"b": 5}}, dict_mod=True)
    assert test_job.function_kwargs["b"] == 7


def test_update_maker_kwargs():
    from dataclasses import dataclass

    from jobflow import Job, Maker, job

    @dataclass
    class AddMaker(Maker):
        name: str = "add"
        b: int = 2

        @job
        def make(self, a):
            return a + self.b

    @job
    def outer_job(maker1, maker2=None):
        pass

    # test no maker
    test_job = Job(add, function_args=(1,), function_kwargs={"b": 2})
    test_job.update_maker_kwargs({"b": 5})
    assert test_job.function_kwargs["b"] == 2

    add_maker = AddMaker(b=3)

    # test no filter
    test_job = add_maker.make(5)
    test_job.update_maker_kwargs({"b": 10})
    assert test_job.maker.b == 10

    # test bad kwarg
    test_job = add_maker.make(5)
    with pytest.raises(TypeError):
        test_job.update_maker_kwargs({"c": 10})

    # test name filter
    test_job = add_maker.make(5)
    test_job.update_maker_kwargs({"b": 10}, name_filter="add")
    assert test_job.maker.b == 10

    test_job = add_maker.make(5)
    test_job.update_maker_kwargs({"b": 10}, name_filter="div")
    assert test_job.maker.b == 3

    # test class filter
    test_job = add_maker.make(5)
    test_job.update_maker_kwargs({"b": 10}, class_filter=AddMaker)
    assert test_job.maker.b == 10

    test_job = add_maker.make(5)
    test_job.update_maker_kwargs({"b": 10}, class_filter=list)
    assert test_job.maker.b == 3

    # test class filter with instance
    test_job = add_maker.make(5)
    test_job.update_maker_kwargs({"b": 10}, class_filter=add_maker)
    assert test_job.maker.b == 10

    # test dict mod
    test_job = add_maker.make(5)
    test_job.update_maker_kwargs({"_inc": {"b": 10}}, dict_mod=True)
    assert test_job.maker.b == 13

    # test makers in job args and kwargs
    maker1 = AddMaker(b=1)
    maker2 = AddMaker(b=2)
    test_job = outer_job(maker1, maker2=maker2)
    test_job.update_maker_kwargs({"_inc": {"b": 10}}, dict_mod=True)
    assert test_job.function_args[0].b == 11
    assert test_job.function_kwargs["maker2"].b == 12

    # test nested=False does not impact the jobs
    test_job.update_maker_kwargs({"_inc": {"b": 20}}, dict_mod=True, nested=False)
    assert test_job.function_args[0].b == 11
    assert test_job.function_kwargs["maker2"].b == 12


def test_append_name():
    from jobflow import Job

    # test append
    test_job = Job(function=add)
    test_job.append_name(" test")
    assert test_job.name == "add test"

    # test prepend
    test_job = Job(function=add)
    test_job.append_name("test ", prepend=True)
    assert test_job.name == "test add"


def test_output_schema(memory_jobstore):
    from pydantic import BaseModel

    from jobflow import Job, Response, job

    class AddSchema(BaseModel):
        result: int

    @job(output_schema=AddSchema)
    def add_schema(a, b):
        return AddSchema(result=a + b)

    @job(output_schema=AddSchema)
    def add_schema_dict(a, b):
        return {"result": a + b}

    @job(output_schema=AddSchema)
    def add_schema_bad(a, b):
        return a + b

    @job(output_schema=AddSchema)
    def add_schema_wrong_key(a, b):
        return {"bad_key": a + b}

    @job(output_schema=AddSchema)
    def add_schema_no_output(a, b):
        a + b

    @job(output_schema=AddSchema)
    def add_schema_response_dict(a, b):
        return Response(output={"result": a + b})

    @job(output_schema=AddSchema)
    def add_schema_response(a, b):
        return Response(output=AddSchema(result=a + b))

    @job
    def add_schema_replace(a, b):
        new_job = Job(add, function_args=(a, b), output_schema=AddSchema)
        return Response(replace=new_job)

    test_job = add_schema(5, 6)
    response = test_job.run(memory_jobstore)
    assert type(response.output).__name__ == "AddSchema"
    assert response.output.result == 11

    test_job = add_schema_dict(5, 6)
    response = test_job.run(memory_jobstore)
    assert type(response.output).__name__ == "AddSchema"
    assert response.output.result == 11

    test_job = add_schema_response(5, 6)
    response = test_job.run(memory_jobstore)
    assert type(response.output).__name__ == "AddSchema"
    assert response.output.result == 11

    test_job = add_schema_response_dict(5, 6)
    response = test_job.run(memory_jobstore)
    assert type(response.output).__name__ == "AddSchema"
    assert response.output.result == 11

    test_job = add_schema_replace(5, 6)
    response = test_job.run(memory_jobstore)
    assert response.replace[-1].output_schema.__name__ == "AddSchema"

    test_job = add_schema_bad(5, 6)
    with pytest.raises(
        ValueError,
        match="Expected output to be AddSchema or dict but got output type of int",
    ):
        test_job.run(memory_jobstore)

    test_job = add_schema_wrong_key(5, 6)
    with pytest.raises(ValueError, match="1 validation error for AddSchema\n"):
        test_job.run(memory_jobstore)

    test_job = add_schema_no_output(5, 6)
    with pytest.raises(
        ValueError, match="Expected output of type AddSchema but got no output"
    ):
        test_job.run(memory_jobstore)


def test_store_inputs(memory_jobstore):
    from jobflow.core.job import Job, OutputReference, store_inputs

    test_job = store_inputs(1)
    test_job.run(memory_jobstore)
    output = memory_jobstore.query_one({"uuid": test_job.uuid}, ["output"])["output"]
    assert output == 1

    ref = OutputReference("abcd")
    test_job = store_inputs(ref)
    test_job.run(memory_jobstore)
    output = memory_jobstore.query_one({"uuid": test_job.uuid}, ["output"])["output"]
    assert OutputReference.from_dict(output) == ref

    # test error msg for multiple stores
    with pytest.raises(
        ValueError, match="Cannot select True for multiple additional stores"
    ):
        _ = Job(function=sum, function_args=([1, 2],), store1=True, store2=True)


def test_pass_manager_config():
    from jobflow import Flow, Job
    from jobflow.core.job import pass_manager_config

    manager_config = {"abc": 1}

    # test single job
    test_job1 = Job(add, function_args=(1,))
    pass_manager_config(test_job1, manager_config)
    assert test_job1.config.manager_config == manager_config

    # test list job
    test_job1 = Job(add, function_args=(1,))
    test_job2 = Job(add, function_args=(1,))
    pass_manager_config([test_job1, test_job2], manager_config)
    assert test_job1.config.manager_config == manager_config
    assert test_job2.config.manager_config == manager_config

    # test flow
    test_job1 = Job(add, function_args=(1,))
    test_job2 = Job(add, function_args=(1,))
    flow = Flow([test_job1, test_job2])
    pass_manager_config(flow, manager_config)
    assert test_job1.config.manager_config == manager_config
    assert test_job2.config.manager_config == manager_config

    # test bad input
    with pytest.raises(ValueError, match="Unrecognised jobs format"):
        pass_manager_config(["str"], manager_config)


def test_hosts(memory_jobstore):
    from jobflow import Job

    test_job = Job(function=add, function_args=(1,))

    assert test_job.hosts == []
    test_job.add_hosts_uuids("12345")
    assert test_job.hosts == ["12345"]

    test_job.add_hosts_uuids(["67890"])
    assert test_job.hosts == ["12345", "67890"]

    test_job.add_hosts_uuids(["09876"], prepend=True)
    assert test_job.hosts == ["09876", "12345", "67890"]

    test_job.run(memory_jobstore)
    result = memory_jobstore.query_one({"uuid": test_job.uuid})
    assert result["hosts"] == ["09876", "12345", "67890"]


def test_update_metadata(memory_jobstore):
    from dataclasses import dataclass

    from jobflow import Job, Maker, Response, job

    # test no filter
    test_job = Job(add, function_args=(1,))
    test_job.update_metadata({"b": 5})
    assert test_job.metadata["b"] == 5
    assert len(test_job.metadata_updates) == 1
    test_job.update_metadata({"c": 6}, dynamic=False)
    assert test_job.metadata["c"] == 6
    assert len(test_job.metadata_updates) == 1

    # test name filter
    test_job = Job(add, function_args=(1,))
    test_job.update_metadata({"b": 5}, name_filter="add")
    assert test_job.metadata["b"] == 5

    test_job = Job(add, function_args=(1,))
    test_job.metadata = {"b": 2}
    test_job.update_metadata({"b": 5}, name_filter="div")
    assert test_job.metadata["b"] == 2

    # test function filter
    test_job = Job(add, function_args=(1,))
    test_job.update_metadata({"b": 5}, function_filter=add)
    assert test_job.metadata["b"] == 5

    test_job = Job(add, function_args=(1,))
    test_job.metadata = {"b": 2}
    test_job.update_metadata({"b": 5}, function_filter=list)
    assert test_job.metadata["b"] == 2

    # test function filter with wrapped job functions
    @job
    def add_wrapped(a, b):
        return a + b

    class A:
        @classmethod
        @job
        def cmj_wrapped(cls, a, b):
            return a + b

        @staticmethod
        @job
        def smj_wrapped(a, b):
            return a + b

        @job
        @classmethod
        def jcm_wrapped(cls, a, b):
            return a + b

        @job
        @staticmethod
        def jsm_wrapped(a, b):
            return a + b

    test_job = add_wrapped(1, 2)
    test_job.update_metadata({"b": 5}, function_filter=add_wrapped)
    assert test_job.metadata["b"] == 5

    test_job = add_wrapped(1, 2)
    test_job.metadata = {"b": 2}
    test_job.update_metadata({"b": 5}, function_filter=add)
    assert test_job.metadata["b"] == 2

    test_job = A.cmj_wrapped(1, 2)
    test_job.update_metadata({"b": 5}, function_filter=A.cmj_wrapped)
    assert test_job.metadata["b"] == 5

    test_job = A.smj_wrapped(1, 2)
    test_job.update_metadata({"b": 5}, function_filter=A.smj_wrapped)
    assert test_job.metadata["b"] == 5

    test_job = A.jcm_wrapped(1, 2)
    test_job.update_metadata({"b": 5}, function_filter=A.jcm_wrapped)
    assert test_job.metadata["b"] == 5

    test_job = A.jsm_wrapped(1, 2)
    test_job.update_metadata({"b": 5}, function_filter=A.jsm_wrapped)
    assert test_job.metadata["b"] == 5

    # test callback filter with complex conditions
    test_job = Job(add, function_args=(1,))
    test_job.metadata = {"x": 1, "y": 2}
    test_job.name = "test_name"

    # Test multiple metadata keys
    test_job.update_metadata(
        {"z": 3},
        callback_filter=lambda job: (
            all(key in job.metadata for key in ["x", "y"])
            and job.name == "test_name"
            and isinstance(job.function_args[0], int)
        ),
    )
    assert test_job.metadata["z"] == 3

    # Test callback filter with no match due to complex condition
    test_job = Job(add, function_args=(1,))
    test_job.metadata = {"x": 1}
    test_job.name = "test_name"
    test_job.update_metadata(
        {"z": 3},
        callback_filter=lambda job: (
            all(key in job.metadata for key in ["x", "y"]) and job.name == "test_name"
        ),
    )
    assert "z" not in test_job.metadata

    # Test callback filter with function argument inspection
    test_job = Job(add, function_args=(1, 2))
    test_job.update_metadata(
        {"w": 4},
        callback_filter=lambda job: (
            len(job.function_args) == 2
            and all(isinstance(arg, int) for arg in job.function_args)
        ),
    )
    assert test_job.metadata["w"] == 4

    # Test callback filter with maker attributes
    @dataclass
    class SpecialMaker(Maker):
        name: str = "special"
        value: int = 42

        @job
        def make(self):
            return 1

    maker = SpecialMaker()
    test_job = maker.make()
    test_job.update_metadata(
        {"v": 5},
        callback_filter=lambda job: (job.maker is not None and job.maker.value == 42),
    )
    assert test_job.metadata["v"] == 5

    # Test callback filter with dynamic updates and complex conditions
    @job
    def use_maker(maker):
        return Response(replace=maker.make())

    test_job = use_maker(SpecialMaker())
    test_job.update_metadata(
        {"u": 6},
        callback_filter=lambda job: (
            hasattr(job, "maker") and getattr(job.maker, "name", "") == "special"
        ),
        dynamic=True,
    )
    response = test_job.run(memory_jobstore)
    assert "u" not in test_job.metadata  # Original job shouldn't match
    assert response.replace[0].metadata["u"] == 6  # But replacement should
    assert any(
        "callback_filter" in update and update["update"].get("u") == 6
        for update in response.replace[0].metadata_updates
    )

    # Test callback filter with function inspection
    def has_specific_signature(job):
        import inspect

        sig = inspect.signature(job.function)
        return len(sig.parameters) == 2 and "b" in sig.parameters

    test_job = Job(add, function_args=(1,))
    test_job.update_metadata({"t": 7}, callback_filter=has_specific_signature)
    assert test_job.metadata["t"] == 7


def test_update_config(memory_jobstore):
    from dataclasses import dataclass

    from jobflow import Job, JobConfig, Maker, Response, job

    new_config = JobConfig(
        resolve_references=False,
        manager_config={"a": "b"},
        pass_manager_config=False,
    )
    new_config_dict = {
        "resolve_references": False,
        "manager_config": {"a": "b"},
        "pass_manager_config": False,
    }

    # test no filter
    test_job = Job(add)
    test_job.update_config(new_config)
    assert test_job.config == new_config
    assert len(test_job.config_updates) == 1
    test_job.update_config(new_config, dynamic=False)
    assert len(test_job.config_updates) == 1

    # test name filter
    test_job = Job(add)
    test_job.update_config(new_config, name_filter="add")
    assert test_job.config == new_config

    test_job = Job(add)
    test_job.update_config(new_config, name_filter="div")
    assert test_job.config != new_config

    # test function filter
    test_job = Job(add)
    test_job.update_config(new_config, function_filter=add)
    assert test_job.config == new_config

    test_job = Job(add)
    test_job.update_config(new_config, function_filter=list)
    assert test_job.config != new_config

    # test function filter with wrapped job functions
    @job
    def add_wrapped(a, b):
        return a + b

    class A:
        @classmethod
        @job
        def cmj_wrapped(cls, a, b):
            return a + b

        @staticmethod
        @job
        def smj_wrapped(a, b):
            return a + b

        @job
        @classmethod
        def jcm_wrapped(cls, a, b):
            return a + b

        @job
        @staticmethod
        def jsm_wrapped(a, b):
            return a + b

    test_job = add_wrapped(1, 2)
    test_job.update_config(new_config, function_filter=add_wrapped)
    assert test_job.config == new_config

    test_job = add_wrapped(1, 2)
    test_job.update_config(new_config, function_filter=list)
    assert test_job.config != new_config

    test_job = A.cmj_wrapped(1, 2)
    test_job.update_config(new_config, function_filter=A.cmj_wrapped)
    assert test_job.config == new_config

    test_job = A.smj_wrapped(1, 2)
    test_job.update_config(new_config, function_filter=A.smj_wrapped)
    assert test_job.config == new_config

    test_job = A.jcm_wrapped(1, 2)
    test_job.update_config(new_config, function_filter=A.jcm_wrapped)
    assert test_job.config == new_config

    test_job = A.jsm_wrapped(1, 2)
    test_job.update_config(new_config, function_filter=A.jsm_wrapped)
    assert test_job.config == new_config

    # test attributes
    test_job = Job(add)
    test_job.update_config(new_config, attributes="manager_config")
    assert test_job.config.manager_config == {"a": "b"}
    assert test_job.config.resolve_references

    test_job = Job(add)
    test_job.update_config(new_config, attributes=["manager_config"])
    assert test_job.config.manager_config == {"a": "b"}
    assert test_job.config.resolve_references

    test_job = Job(add)
    test_job.update_config(
        new_config, attributes=["manager_config", "resolve_references"]
    )
    assert test_job.config.manager_config == {"a": "b"}
    assert not test_job.config.resolve_references
    assert test_job.config.pass_manager_config

    with pytest.raises(ValueError, match="Unknown JobConfig attribute: abc_xyz"):
        test_job.update_config(new_config, attributes="abc_xyz")

    # test dictionary config updates
    test_job = Job(add)
    test_job.update_config(new_config_dict)
    assert test_job.config == new_config

    # test dict with attributes
    test_job = Job(add)
    test_job.update_config(new_config_dict, attributes="manager_config")
    assert test_job.config.manager_config == {"a": "b"}
    assert test_job.config.resolve_references

    test_job = Job(add)
    test_job.update_config(new_config_dict, attributes=["manager_config"])
    assert test_job.config.manager_config == {"a": "b"}
    assert test_job.config.resolve_references

    test_job = Job(add)
    test_job.update_config(
        new_config_dict, attributes=["manager_config", "resolve_references"]
    )
    assert test_job.config.manager_config == {"a": "b"}
    assert not test_job.config.resolve_references
    assert test_job.config.pass_manager_config

    with pytest.raises(
        ValueError,
        match="Specified attributes include a key that is not present in the config",
    ):
        test_job.update_config(new_config_dict, attributes="abc_xyz")

    # test applied dynamic updates
    @dataclass
    class TestMaker(Maker):
        name = "test"

        @job
        def make(self, a, b):
            return a + b

    @job
    def use_maker(maker):
        return Response(replace=maker.make())

    test_job = use_maker(TestMaker())
    test_job.name = "use"
    test_job.update_config(new_config, name_filter="test")
    assert test_job.config != new_config
    response = test_job.run(memory_jobstore)
    assert response.replace[0].config == new_config
    assert response.replace[0].config_updates[0]["config"] == new_config


def test_job_magic_methods():
    from jobflow import Job

    # prepare test jobs
    job1 = Job(function=sum, function_args=([1, 2],))
    job2 = Job(function=dict, function_args=((("a", 1), ("b", 2)),))
    job3 = Job(function=sum, function_args=([1, 2],))

    # test __repr__
    assert repr(job1) == f"Job(name='sum', uuid='{job1.uuid}')"
    assert repr(job2) == f"Job(name='dict', uuid='{job2.uuid}')"
    assert repr(job3) == f"Job(name='sum', uuid='{job3.uuid}')"
    assert repr(job1) != repr(job3)

    # test __contains__ (using some fake UUID)
    # initial job.input_references is empty so can't test positive case
    assert "fake-uuid" not in job1

    # test __eq__
    assert job1 == job1  # noqa: PLR0124
    assert job2 == job2  # noqa: PLR0124
    assert job1 != job2
    assert job1 != job3  # Different UUIDs

    # test __hash__
    assert hash(job1) != hash(job2) != hash(job3)
