import pytest


def add(a, b):
    return a + b


def div(a, b=2):
    return a / b


def get_test_job():
    from jobflow import Job

    return Job(add, function_args=(1, 2))


def get_test_flow():
    from jobflow import Flow, Job

    add_job = Job(add, function_args=(1, 2))
    div_job = Job(div, function_args=(add_job.output,), function_kwargs={"b": 3})
    div_job.metadata = {"b": 3}
    return Flow([add_job, div_job])


def get_maker_flow(return_makers=False):
    from dataclasses import dataclass

    from jobflow import Flow, Maker, job

    @dataclass
    class AddMaker(Maker):
        name: str = "add"
        b: int = 2

        @job
        def make(self, a):
            return a + self.b

    @dataclass
    class DivMaker(Maker):
        name: str = "div"
        b: int = 5

        @job
        def make(self, a):
            return a / self.b

    add_maker = AddMaker(b=3)
    div_maker = DivMaker(b=4)
    add_job = add_maker.make(2)
    div_job = div_maker.make(add_job.output)
    flow = Flow([add_job, div_job])

    if return_makers:
        return flow, (AddMaker, DivMaker)
    else:
        return flow


def test_flow_of_jobs_init():
    from jobflow.core.flow import Flow, JobOrder

    # test single job
    add_job = get_test_job()
    flow = Flow([add_job], name="add")
    assert flow.name == "add"
    assert flow.host is None
    assert flow.output is None
    assert flow.job_uuids == (add_job.uuid,)
    assert flow.all_uuids == (add_job.uuid,)

    # test single job no list
    add_job = get_test_job()
    flow = Flow(add_job, name="add")
    assert flow.name == "add"
    assert flow.host is None
    assert flow.output is None
    assert flow.job_uuids == (add_job.uuid,)

    # # test multiple job
    add_job1 = get_test_job()
    add_job2 = get_test_job()
    flow = Flow([add_job1, add_job2])
    assert flow.host is None
    assert flow.output is None
    assert flow.job_uuids == (add_job1.uuid, add_job2.uuid)
    assert flow.all_uuids == (add_job1.uuid, add_job2.uuid)

    # # test multiple job, linear order
    add_job1 = get_test_job()
    add_job2 = get_test_job()
    flow = Flow([add_job1, add_job2], order=JobOrder.LINEAR)
    assert flow.host is None
    assert flow.output is None
    assert flow.job_uuids == (add_job1.uuid, add_job2.uuid)

    # test single job and output
    add_job = get_test_job()
    flow = Flow([add_job], output=add_job.output)
    assert flow.output == add_job.output

    # # test multi job and list multi outputs
    add_job1 = get_test_job()
    add_job2 = get_test_job()
    flow = Flow([add_job1, add_job2], output=[add_job1.output, add_job2.output])
    assert flow.output[1] == add_job2.output

    # # test all jobs included needed to generate outputs
    add_job = get_test_job()
    with pytest.raises(ValueError):
        Flow([], output=add_job.output)

    # test job given rather than outputs
    add_job = get_test_job()
    with pytest.warns(UserWarning):
        Flow([add_job], output=add_job)

    # test complex object containing job given rather than outputs
    add_job = get_test_job()
    with pytest.warns(UserWarning):
        Flow([add_job], output={1: [[{"a": add_job}]]})

    # test job already belongs to another flow
    add_job = get_test_job()
    Flow([add_job])
    with pytest.raises(ValueError):
        Flow([add_job])

    # test that two of the same job cannot be used in the same flow
    add_job = get_test_job()
    with pytest.raises(ValueError):
        Flow([add_job, add_job])


def test_flow_of_flows_init():
    from jobflow.core.flow import Flow

    # test single flow
    add_job = get_test_job()
    subflow = Flow([add_job])
    flow = Flow([subflow], name="add")
    assert flow.name == "add"
    assert flow.host is None
    assert flow.output is None
    assert flow.job_uuids == (add_job.uuid,)
    assert flow.all_uuids == (add_job.uuid, subflow.uuid)
    assert flow.jobs[0].host == flow.uuid

    # test single flow no list
    add_job = get_test_job()
    subflow = Flow(add_job)
    flow = Flow(subflow, name="add")
    assert flow.name == "add"
    assert flow.host is None
    assert flow.output is None
    assert flow.job_uuids == (add_job.uuid,)
    assert flow.jobs[0].host == flow.uuid

    # test multiple flows
    add_job1 = get_test_job()
    add_job2 = get_test_job()
    subflow1 = Flow([add_job1])
    subflow2 = Flow([add_job2])
    flow = Flow([subflow1, subflow2])
    assert flow.host is None
    assert flow.output is None
    assert flow.job_uuids == (add_job1.uuid, add_job2.uuid)
    assert flow.all_uuids == (
        add_job1.uuid,
        subflow1.uuid,
        add_job2.uuid,
        subflow2.uuid,
    )
    assert flow.jobs[0].host == flow.uuid
    assert flow.jobs[1].host == flow.uuid

    # test single job and outputs
    add_job = get_test_job()
    subflow = Flow([add_job], output=add_job.output)
    flow = Flow([subflow], output=subflow.output)
    assert flow.output == add_job.output

    # test multi job and list multi outputs
    add_job1 = get_test_job()
    add_job2 = get_test_job()
    subflow1 = Flow([add_job1], output=add_job1.output)
    subflow2 = Flow([add_job2], output=add_job2.output)
    flow = Flow([subflow1, subflow2], output=[subflow1.output, subflow2.output])
    assert flow.output[0] == add_job1.output
    assert flow.output[1] == add_job2.output

    # test all jobflow included needed to generate outputs
    add_job = get_test_job()
    subflow = Flow([add_job], output=add_job.output)
    with pytest.raises(ValueError):
        Flow([], output=subflow.output)

    # test flow given rather than outputs
    add_job = get_test_job()
    subflow = Flow([add_job], output=add_job.output)
    with pytest.warns(UserWarning):
        Flow([subflow], output=subflow)

    # test complex object containing job given rather than outputs
    add_job = get_test_job()
    subflow = Flow([add_job], output=add_job.output)
    with pytest.warns(UserWarning):
        Flow([subflow], output={1: [[{"a": subflow}]]})

    # test flow already belongs to another flow
    add_job = get_test_job()
    subflow = Flow([add_job], output=add_job.output)
    Flow([subflow])
    with pytest.raises(ValueError):
        Flow([subflow])

    # test that two of the same flow cannot be used in the same flow
    add_job = get_test_job()
    subflow = Flow([add_job], output=add_job.output)
    with pytest.raises(ValueError):
        Flow([subflow, subflow])


def test_flow_job_mixed():
    from jobflow.core.flow import Flow

    # test job and flows
    add_job = get_test_job()
    add_job2 = get_test_job()
    subflow = Flow([add_job2])
    flow = Flow([add_job, subflow])
    assert flow.host is None
    assert flow.output is None
    assert flow.job_uuids == (add_job.uuid, add_job2.uuid)
    assert flow.jobs[0].host == flow.uuid
    assert flow.jobs[1].host == flow.uuid

    # test with list multi outputs
    add_job = get_test_job()
    add_job2 = get_test_job()
    subflow = Flow([add_job2], output=add_job2.output)
    flow = Flow([add_job, subflow], output=[add_job.output, subflow.output])
    assert flow.output[0] == add_job.output
    assert flow.output[1] == add_job2.output

    # test all jobs/flows included needed to generate outputs
    add_job = get_test_job()
    add_job2 = get_test_job()
    subflow = Flow([add_job2], output=add_job2.output)
    with pytest.raises(ValueError):
        Flow([add_job], output=[add_job.output, subflow.output])


def test_graph():
    from jobflow import Flow, JobOrder

    # test unconnected graph
    add_job1 = get_test_job()
    add_job2 = get_test_job()
    flow = Flow([add_job1, add_job2])
    graph = flow.graph
    assert len(graph.edges) == 0
    assert len(graph.nodes) == 2

    # test unconnected graph, linear order
    add_job1 = get_test_job()
    add_job2 = get_test_job()
    flow = Flow([add_job1, add_job2], order=JobOrder.LINEAR)
    graph = flow.graph
    assert len(graph.edges) == 1
    assert len(graph.nodes) == 2

    # test connected graph, wrong order
    add_job1 = get_test_job()
    add_job2 = get_test_job()
    add_job1.function_args = (2, add_job2.output)
    flow = Flow([add_job1, add_job2])
    graph = flow.graph
    assert len(graph.edges) == 1
    assert len(graph.nodes) == 2

    # test connected graph, linear order
    add_job1 = get_test_job()
    add_job2 = get_test_job()
    add_job1.function_args = (2, add_job2.output)
    flow = Flow([add_job1, add_job2], order=JobOrder.LINEAR)
    graph = flow.graph
    assert len(graph.edges) == 2
    assert len(graph.nodes) == 2

    # test unconnected graph
    add_job1 = get_test_job()
    add_job2 = get_test_job()
    add_job3 = get_test_job()
    add_job4 = get_test_job()
    subflow1 = Flow([add_job1, add_job2])
    subflow2 = Flow([add_job3, add_job4])
    flow = Flow([subflow1, subflow2])
    graph = flow.graph
    assert len(graph.edges) == 0
    assert len(graph.nodes) == 4

    # test unconnected graph, linear order
    add_job1 = get_test_job()
    add_job2 = get_test_job()
    add_job3 = get_test_job()
    add_job4 = get_test_job()
    subflow1 = Flow([add_job1, add_job2])
    subflow2 = Flow([add_job3, add_job4])
    flow = Flow([subflow1, subflow2], order=JobOrder.LINEAR)
    graph = flow.graph
    assert len(graph.edges) == 4
    assert len(graph.nodes) == 4

    # test connected graph, wrong order
    add_job1 = get_test_job()
    add_job2 = get_test_job()
    add_job3 = get_test_job()
    add_job4 = get_test_job()
    subflow1 = Flow([add_job1, add_job2])
    subflow2 = Flow([add_job3, add_job4])
    add_job1.function_args = (2, add_job3.output)
    flow = Flow([subflow1, subflow2])
    graph = flow.graph
    assert len(graph.edges) == 1
    assert len(graph.nodes) == 4

    # test connected graph, linear order
    add_job1 = get_test_job()
    add_job2 = get_test_job()
    add_job3 = get_test_job()
    add_job4 = get_test_job()
    subflow1 = Flow([add_job1, add_job2])
    subflow2 = Flow([add_job3, add_job4])
    add_job1.function_args = (2, add_job3.output)
    flow = Flow([subflow1, subflow2], order=JobOrder.LINEAR)
    graph = flow.graph
    assert len(graph.edges) == 5
    assert len(graph.nodes) == 4


def test_draw_graph():
    from jobflow import Flow, JobOrder

    # test unconnected graph
    add_job1 = get_test_job()
    add_job2 = get_test_job()
    flow = Flow([add_job1, add_job2])
    assert flow.draw_graph()

    # test unconnected graph, linear order
    add_job1 = get_test_job()
    add_job2 = get_test_job()
    flow = Flow([add_job1, add_job2], order=JobOrder.LINEAR)
    assert flow.draw_graph()

    # test connected graph, wrong order
    add_job1 = get_test_job()
    add_job2 = get_test_job()
    add_job1.function_args = (2, add_job2.output)
    flow = Flow([add_job1, add_job2])
    assert flow.draw_graph()

    # test connected graph, linear order
    add_job1 = get_test_job()
    add_job2 = get_test_job()
    add_job1.function_args = (2, add_job2.output)
    flow = Flow([add_job1, add_job2], order=JobOrder.LINEAR)
    assert flow.draw_graph()


@pytest.mark.usefixtures("no_pydot")
def test_draw_graph_nopydot():
    from jobflow import Flow, JobOrder

    # test unconnected graph
    add_job1 = get_test_job()
    add_job2 = get_test_job()
    flow = Flow([add_job1, add_job2])
    assert flow.draw_graph()

    # test unconnected graph, linear order
    add_job1 = get_test_job()
    add_job2 = get_test_job()
    flow = Flow([add_job1, add_job2], order=JobOrder.LINEAR)
    assert flow.draw_graph()

    # test connected graph, wrong order
    add_job1 = get_test_job()
    add_job2 = get_test_job()
    add_job1.function_args = (2, add_job2.output)
    flow = Flow([add_job1, add_job2])
    assert flow.draw_graph()

    # test connected graph, linear order
    add_job1 = get_test_job()
    add_job2 = get_test_job()
    add_job1.function_args = (2, add_job2.output)
    flow = Flow([add_job1, add_job2], order=JobOrder.LINEAR)
    assert flow.draw_graph()


def test_iterflow():
    from jobflow import Flow, JobOrder

    # test unconnected graph
    add_job1 = get_test_job()
    add_job2 = get_test_job()
    flow = Flow([add_job1, add_job2])
    iterated = list(flow.iterflow())
    assert len(iterated) == 2
    assert iterated[0][0] == add_job1
    assert len(iterated[0][1]) == 0
    assert iterated[1][0] == add_job2
    assert len(iterated[1][1]) == 0

    # test unconnected graph, linear order
    add_job1 = get_test_job()
    add_job2 = get_test_job()
    flow = Flow([add_job1, add_job2], order=JobOrder.LINEAR)
    iterated = list(flow.iterflow())
    assert len(iterated) == 2
    assert iterated[0][0] == add_job1
    assert len(iterated[0][1]) == 0
    assert iterated[1][0] == add_job2
    assert len(iterated[1][1]) == 1

    # test connected graph, wrong order
    add_job1 = get_test_job()
    add_job2 = get_test_job()
    add_job1.function_args = (2, add_job2.output)
    flow = Flow([add_job1, add_job2])
    iterated = list(flow.iterflow())
    assert len(iterated) == 2
    assert iterated[0][0] == add_job2
    assert len(iterated[0][1]) == 0
    assert iterated[1][0] == add_job1
    assert len(iterated[1][1]) == 1

    # test connected graph, linear order
    add_job1 = get_test_job()
    add_job2 = get_test_job()
    add_job1.function_args = (2, add_job2.output)
    flow = Flow([add_job1, add_job2], order=JobOrder.LINEAR)
    with pytest.raises(ValueError):
        list(flow.iterflow())


def test_dag_validation():
    from jobflow import Flow, Job

    # test cycle detection of jobs
    job1 = Job(add, function_args=(1, 2))
    job2 = Job(add, function_args=(job1.output, 2))
    job1.function_args = (job2.output, 2)
    flow = Flow(jobs=[job1, job2])
    with pytest.raises(ValueError):
        next(flow.iterflow())


def test_serialization():
    import json

    from monty.json import MontyDecoder, MontyEncoder

    from jobflow import Flow

    flow = Flow([])
    flow_host = Flow([flow])
    host_uuid = flow_host.uuid

    encoded_flow = json.loads(MontyEncoder().encode(flow_host))
    decoded_flow = MontyDecoder().process_decoded(encoded_flow)

    assert decoded_flow.jobs[0].host == host_uuid


def test_update_kwargs():
    # test no filter
    flow = get_test_flow()
    flow.update_kwargs({"b": 5})
    assert flow.jobs[0].function_kwargs["b"] == 5
    assert flow.jobs[1].function_kwargs["b"] == 5

    # test name filter
    flow = get_test_flow()
    flow.update_kwargs({"b": 5}, name_filter="div")
    assert "b" not in flow.jobs[0].function_kwargs
    assert flow.jobs[1].function_kwargs["b"] == 5

    # test function filter
    flow = get_test_flow()
    flow.update_kwargs({"b": 5}, function_filter=div)
    assert "b" not in flow.jobs[0].function_kwargs
    assert flow.jobs[1].function_kwargs["b"] == 5

    # test dict mod
    flow = get_test_flow()
    flow.update_kwargs({"_inc": {"b": 5}}, function_filter=div, dict_mod=True)
    assert "b" not in flow.jobs[0].function_kwargs
    assert flow.jobs[1].function_kwargs["b"] == 8


def test_update_maker_kwargs():

    # test no filter
    flow = get_maker_flow()
    flow.update_maker_kwargs({"b": 10})
    assert flow.jobs[0].maker.b == 10
    assert flow.jobs[1].maker.b == 10

    # test bad kwarg
    flow = get_maker_flow()
    with pytest.raises(TypeError):
        flow.update_maker_kwargs({"c": 10})

    # test name filter
    flow = get_maker_flow()
    flow.update_maker_kwargs({"b": 10}, name_filter="div")
    assert flow.jobs[0].maker.b == 3
    assert flow.jobs[1].maker.b == 10

    # test class filter
    flow, (_, DivMaker) = get_maker_flow(return_makers=True)
    flow.update_maker_kwargs({"b": 10}, class_filter=DivMaker)
    assert flow.jobs[0].maker.b == 3
    assert flow.jobs[1].maker.b == 10

    # test class filter with instance
    flow, (_, DivMaker) = get_maker_flow(return_makers=True)
    div_maker = DivMaker()
    flow.update_maker_kwargs({"b": 10}, class_filter=div_maker)
    assert flow.jobs[0].maker.b == 3
    assert flow.jobs[1].maker.b == 10

    # test dict mod
    flow = get_maker_flow()
    flow.update_maker_kwargs({"_inc": {"b": 10}}, name_filter="div", dict_mod=True)
    assert flow.jobs[0].maker.b == 3
    assert flow.jobs[1].maker.b == 14


def test_append_name():
    from jobflow import Flow

    # test append
    flow = get_test_flow()
    flow.append_name(" test")
    assert flow.name == "Flow test"
    assert flow.jobs[0].name == "add test"

    # test prepend
    flow = get_test_flow()
    flow.append_name("test ", prepend=True)
    assert flow.name == "test Flow"
    assert flow.jobs[0].name == "test add"

    # test empty Flow
    flow = Flow([], name="abc")
    flow.append_name(" test")
    assert flow.name == "abc test"


def test_get_flow():
    import jobflow
    from jobflow import Job
    from jobflow.core.flow import get_flow

    # test get_flow method with a single job
    job = get_test_job()
    assert isinstance(job, jobflow.Job)
    flow = get_flow(job)
    assert isinstance(flow, jobflow.Flow)
    assert flow.jobs[0] is job

    # test get_flow method with a list of jobs
    job1 = get_test_job()
    job2 = get_test_job()
    flow = get_flow([job1, job2])
    assert isinstance(flow, jobflow.Flow)
    assert len(flow.jobs) == 2
    assert flow.jobs[0] is job1
    assert flow.jobs[1] is job2

    # test get_flow method with a flow
    flw = get_test_flow()
    assert isinstance(flw, jobflow.Flow)
    flow = get_flow(flw)
    assert isinstance(flow, jobflow.Flow)
    assert flow is flw

    # test all jobs included for graph to work
    job1 = Job(add, function_args=(1, 2))
    job2 = Job(add, function_args=(job1.output.value, 2))
    with pytest.raises(ValueError):
        get_flow(job2)

    # test all jobs included for graph to work
    job1 = Job(add, function_args=(1, 2))
    job2 = Job(add, function_args=(job1.output.value, 2))
    get_flow([job1, job2])


def test_add_hosts_uuids():
    from jobflow.core.flow import Flow

    add_job1 = get_test_job()
    add_job2 = get_test_job()

    # test adding host id again (automatically added once on job creation)
    flow = Flow([add_job1, add_job2])
    flow.add_hosts_uuids()
    assert add_job1.hosts == [flow.uuid, flow.uuid]
    assert add_job2.hosts == [flow.uuid, flow.uuid]

    # test appending specific uuid
    flow = Flow([get_test_job()])
    flow.add_hosts_uuids("abc")
    assert flow.jobs[0].hosts == [flow.uuid, "abc"]

    # test appending several uuid
    flow = Flow([get_test_job()])
    flow.add_hosts_uuids(["abc", "xyz"])
    assert flow.jobs[0].hosts == [flow.uuid, "abc", "xyz"]

    # test prepending specific uuid
    flow = Flow([get_test_job()])
    flow.add_hosts_uuids("abc", prepend=True)
    assert flow.jobs[0].hosts == ["abc", flow.uuid]


def test_hosts():
    from jobflow.core.flow import Flow

    # test single job
    add_job1 = get_test_job()
    add_job2 = get_test_job()
    flow1 = Flow(add_job1)
    flow2 = Flow([flow1, add_job2])
    flow3 = Flow(flow2)

    assert add_job1.hosts == [flow1.uuid, flow2.uuid, flow3.uuid]
    assert add_job2.hosts == [flow2.uuid, flow3.uuid]
    assert flow1.hosts == [flow2.uuid, flow3.uuid]
    assert flow2.hosts == [flow3.uuid]
    assert flow3.hosts == []


def test_add_jobs():
    from jobflow.core.flow import Flow

    add_job1 = get_test_job()
    add_job2 = get_test_job()
    flow1 = Flow(add_job1)
    flow1.add_jobs(add_job2)
    assert len(flow1.jobs) == 2
    assert add_job2.hosts == [flow1.uuid]

    with pytest.raises(ValueError):
        flow1.add_jobs(add_job2)

    add_job3 = get_test_job()
    Flow(add_job3)

    # job belongs to another Flow
    with pytest.raises(ValueError):
        flow1.add_jobs(add_job3)

    add_job4 = get_test_job()
    with pytest.raises(ValueError):
        flow1.add_jobs([add_job4, add_job4])

    # nested flows
    inner_flow = Flow(get_test_job())
    outer_flow = Flow(inner_flow)
    assert inner_flow.hosts == [outer_flow.uuid]
    added_job = get_test_job()
    inner_flow.add_jobs(added_job)
    assert added_job.hosts == [inner_flow.uuid, outer_flow.uuid]

    # circular dependency
    flow1 = Flow([get_test_flow()])
    flow2 = Flow([flow1])
    flow3 = Flow([flow2])
    with pytest.raises(ValueError):
        flow1.add_jobs(flow3)


def test_remove_jobs():
    from jobflow.core.flow import Flow

    # test removing one job
    add_job1 = get_test_job()
    add_job2 = get_test_job()
    flow1 = Flow([add_job1, add_job2])

    flow1.remove_jobs(0)
    assert len(flow1.jobs) == 1
    assert flow1.jobs[0].uuid is add_job2.uuid

    with pytest.raises(ValueError):
        flow1.remove_jobs(-1)
    with pytest.raises(ValueError):
        flow1.remove_jobs(10)

    # test removing two jobs
    add_job1 = get_test_job()
    add_job2 = get_test_job()
    add_job3 = get_test_job()
    flow = Flow([add_job1, add_job2, add_job3])

    flow.remove_jobs([0, 2])
    assert len(flow.jobs) == 1
    assert flow.jobs[0].uuid is add_job2.uuid

    # test removing job which is used in output
    add_job1 = get_test_job()
    add_job2 = get_test_job()
    flow2 = Flow([add_job1, add_job2], output=add_job2.output)

    with pytest.raises(ValueError):
        flow2.remove_jobs(1)

    # test removing a flow
    add_job1 = get_test_job()
    add_job2 = get_test_job()
    add_job3 = get_test_job()
    flow_inner = Flow([add_job1, add_job2])
    flow = Flow([flow_inner, add_job3])

    flow.remove_jobs(0)
    assert len(flow.jobs) == 1
    assert flow.jobs[0].uuid is add_job3.uuid

    # test removing a flow which is used in output
    add_job1 = get_test_job()
    add_job2 = get_test_job()
    add_job3 = get_test_job()
    flow_inner = Flow([add_job1, add_job2])
    flow = Flow([flow_inner, add_job3], output=flow_inner.jobs[0].output)

    with pytest.raises(ValueError):
        flow.remove_jobs(0)

    # test removing a job in a flow containing another flow
    add_job1 = get_test_job()
    add_job2 = get_test_job()
    add_job3 = get_test_job()
    flow_inner = Flow([add_job1, add_job2])
    flow = Flow([flow_inner, add_job3])

    flow.remove_jobs(1)
    assert len(flow.jobs) == 1
    assert flow.jobs[0].uuid is flow_inner.uuid


def test_set_output():
    from jobflow.core.flow import Flow

    add_job1 = get_test_job()
    add_job2 = get_test_job()
    add_job3 = get_test_job()
    flow = Flow([add_job1, add_job2], output=add_job2.output)

    flow.output = add_job1.output
    assert flow.output.uuid == add_job1.uuid

    with pytest.raises(ValueError):
        flow.output = [add_job3.output]


def test_update_metadata():
    # test no filter
    flow = get_test_flow()
    flow.update_metadata({"b": 5})
    assert flow.jobs[0].metadata["b"] == 5
    assert flow.jobs[1].metadata["b"] == 5

    # test name filter
    flow = get_test_flow()
    flow.update_metadata({"b": 5}, name_filter="div")
    assert "b" not in flow.jobs[0].metadata
    assert flow.jobs[1].metadata["b"] == 5

    # test function filter
    flow = get_test_flow()
    flow.update_metadata({"b": 5}, function_filter=div)
    assert "b" not in flow.jobs[0].metadata
    assert flow.jobs[1].metadata["b"] == 5

    # test dict mod
    flow = get_test_flow()
    flow.update_metadata({"_inc": {"b": 5}}, function_filter=div, dict_mod=True)
    assert "b" not in flow.jobs[0].metadata
    assert flow.jobs[1].metadata["b"] == 8


def test_update_config():
    from jobflow import JobConfig

    new_config = JobConfig(
        resolve_references=False,
        manager_config={"a": "b"},
        pass_manager_config=False,
    )

    # test no filter
    flow = get_test_flow()
    flow.update_config(new_config)
    assert flow.jobs[0].config == new_config
    assert flow.jobs[1].config == new_config

    # test name filter
    flow = get_test_flow()
    flow.update_config(new_config, name_filter="div")
    assert flow.jobs[0].config != new_config
    assert flow.jobs[1].config == new_config

    # test function filter
    flow = get_test_flow()
    flow.update_config(new_config, function_filter=div)
    assert flow.jobs[0].config != new_config
    assert flow.jobs[1].config == new_config

    # test attributes
    flow = get_test_flow()
    flow.update_config(new_config, function_filter=div, attributes=["manager_config"])
    assert flow.jobs[0].config.manager_config == {}
    assert flow.jobs[0].config.resolve_references
    assert flow.jobs[1].config.manager_config == {"a": "b"}
    assert flow.jobs[1].config.resolve_references
