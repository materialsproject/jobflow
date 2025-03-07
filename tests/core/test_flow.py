import pytest


def add(a, b):
    return a + b


def div(a, b=2):
    return a / b


def get_test_job(*args):
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
    with pytest.raises(
        ValueError, match="jobs array does not contain all jobs needed for flow output"
    ):
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
    with pytest.raises(ValueError, match="already belongs to another flow"):
        Flow([add_job])

    # test that two of the same job cannot be used in the same flow
    add_job = get_test_job()
    with pytest.raises(
        ValueError, match="jobs array contains multiple jobs/flows with the same uuid"
    ):
        Flow([add_job, add_job])


def test_flow_of_flows_init():
    from jobflow.core.flow import Flow

    # test single flow
    add_job = get_test_job()
    sub_flow = Flow([add_job])
    flow = Flow([sub_flow], name="add")
    assert flow.name == "add"
    assert flow.host is None
    assert flow.output is None
    assert flow.job_uuids == (add_job.uuid,)
    assert flow.all_uuids == (add_job.uuid, sub_flow.uuid)
    assert flow[0].host == flow.uuid

    # test single flow no list
    add_job = get_test_job()
    sub_flow = Flow(add_job)
    flow = Flow(sub_flow, name="add")
    assert flow.name == "add"
    assert flow.host is None
    assert flow.output is None
    assert flow.job_uuids == (add_job.uuid,)
    assert flow[0].host == flow.uuid

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
    assert flow[0].host == flow.uuid
    assert flow[1].host == flow.uuid

    # test single job and outputs
    add_job = get_test_job()
    sub_flow = Flow([add_job], output=add_job.output)
    flow = Flow([sub_flow], output=sub_flow.output)
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
    sub_flow = Flow([add_job], output=add_job.output)
    with pytest.raises(ValueError, match="jobs array does not contain all jobs"):
        Flow([], output=sub_flow.output)

    # test flow given rather than outputs
    add_job = get_test_job()
    sub_flow = Flow([add_job], output=add_job.output)
    with pytest.warns(UserWarning):
        Flow([sub_flow], output=sub_flow)

    # test complex object containing job given rather than outputs
    add_job = get_test_job()
    sub_flow = Flow([add_job], output=add_job.output)
    with pytest.warns(UserWarning):
        Flow([sub_flow], output={1: [[{"a": sub_flow}]]})

    # test flow already belongs to another flow
    add_job = get_test_job()
    sub_flow = Flow([add_job], output=add_job.output)
    Flow([sub_flow])
    with pytest.raises(ValueError, match="already belongs to another flow"):
        Flow([sub_flow])

    # test that two of the same flow cannot be used in the same flow
    add_job = get_test_job()
    sub_flow = Flow([add_job], output=add_job.output)
    with pytest.raises(ValueError, match="jobs array contains multiple jobs/flows"):
        Flow([sub_flow, sub_flow])


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
    assert flow[0].host == flow.uuid
    assert flow[1].host == flow.uuid

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
    with pytest.raises(ValueError, match="jobs array does not contain all jobs"):
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

    # test external reference
    add_job1 = get_test_job()
    add_job2 = get_test_job()
    add_job1.function_args = (2, add_job2.output)
    flow = Flow([add_job1])
    graph = flow.graph
    assert len(graph.edges) == 1
    assert len(graph.nodes) == 2


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
    from jobflow import Flow, JobOrder, OutputReference

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
    with pytest.raises(ValueError, match="Job connectivity contains cycles"):
        list(flow.iterflow())

    # test with external reference
    add_job1 = get_test_job()
    add_job1.function_args = (2, OutputReference("a-fake-uuid"))
    flow = Flow([add_job1], order=JobOrder.LINEAR)
    list(flow.iterflow())


def test_dag_validation():
    from jobflow import Flow, Job

    # test cycle detection of jobs
    job1 = Job(add, function_args=(1, 2))
    job2 = Job(add, function_args=(job1.output, 2))
    job1.function_args = (job2.output, 2)
    flow = Flow(jobs=[job1, job2])
    with pytest.raises(
        ValueError,
        match="Job connectivity contains cycles therefore job execution order "
        "cannot be determined",
    ):
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

    assert decoded_flow[0].host == host_uuid
    assert flow_host.jobs[0].hosts == decoded_flow.jobs[0].hosts


def test_update_kwargs():
    # test no filter
    flow = get_test_flow()
    flow.update_kwargs({"b": 5})
    assert flow[0].function_kwargs["b"] == 5
    assert flow[1].function_kwargs["b"] == 5

    # test name filter
    flow = get_test_flow()
    flow.update_kwargs({"b": 5}, name_filter="div")
    assert "b" not in flow[0].function_kwargs
    assert flow[1].function_kwargs["b"] == 5

    # test function filter
    flow = get_test_flow()
    flow.update_kwargs({"b": 5}, function_filter=div)
    assert "b" not in flow[0].function_kwargs
    assert flow[1].function_kwargs["b"] == 5

    # test dict mod
    flow = get_test_flow()
    flow.update_kwargs({"_inc": {"b": 5}}, function_filter=div, dict_mod=True)
    assert "b" not in flow[0].function_kwargs
    assert flow[1].function_kwargs["b"] == 8


def test_update_maker_kwargs():
    # test no filter
    flow = get_maker_flow()
    flow.update_maker_kwargs({"b": 10})
    assert flow[0].maker.b == 10
    assert flow[1].maker.b == 10

    # test bad kwarg
    flow = get_maker_flow()
    with pytest.raises(TypeError):
        flow.update_maker_kwargs({"c": 10})

    # test name filter
    flow = get_maker_flow()
    flow.update_maker_kwargs({"b": 10}, name_filter="div")
    assert flow[0].maker.b == 3
    assert flow[1].maker.b == 10

    # test class filter
    flow, (_, DivMaker) = get_maker_flow(return_makers=True)
    flow.update_maker_kwargs({"b": 10}, class_filter=DivMaker)
    assert flow[0].maker.b == 3
    assert flow[1].maker.b == 10

    # test class filter with instance
    flow, (_, DivMaker) = get_maker_flow(return_makers=True)
    div_maker = DivMaker()
    flow.update_maker_kwargs({"b": 10}, class_filter=div_maker)
    assert flow[0].maker.b == 3
    assert flow[1].maker.b == 10

    # test dict mod
    flow = get_maker_flow()
    flow.update_maker_kwargs({"_inc": {"b": 10}}, name_filter="div", dict_mod=True)
    assert flow[0].maker.b == 3
    assert flow[1].maker.b == 14


def test_append_name():
    from jobflow import Flow

    # test append
    flow = get_test_flow()
    flow.append_name(" test")
    assert flow.name == "Flow test"
    assert flow[0].name == "add test"

    # test prepend
    flow = get_test_flow()
    flow.append_name("test ", prepend=True)
    assert flow.name == "test Flow"
    assert flow[0].name == "test add"

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
    assert flow[0] is job

    # test get_flow method with a list of jobs
    job1 = get_test_job()
    job2 = get_test_job()
    flow = get_flow([job1, job2])
    assert isinstance(flow, jobflow.Flow)
    assert len(flow) == 2
    assert flow[0] is job1
    assert flow[1] is job2

    # test get_flow method with a flow
    flw = get_test_flow()
    assert isinstance(flw, jobflow.Flow)
    flow = get_flow(flw)
    assert isinstance(flow, jobflow.Flow)
    assert flow is flw

    # test all jobs included for graph to work
    job1 = Job(add, function_args=(1, 2))
    job2 = Job(add, function_args=(job1.output.value, 2))
    with pytest.raises(
        ValueError,
        match="The following jobs were not found in the jobs array and are needed to ",
    ):
        get_flow(job2, allow_external_references=False)

    job1 = Job(add, function_args=(1, 2))
    job2 = Job(add, function_args=(job1.output.value, 2))
    assert get_flow(job2, allow_external_references=True)

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
    assert flow[0].hosts == [flow.uuid, "abc"]

    # test appending several uuid
    flow = Flow([get_test_job()])
    flow.add_hosts_uuids(["abc", "xyz"])
    assert flow[0].hosts == [flow.uuid, "abc", "xyz"]

    # test prepending specific uuid
    flow = Flow([get_test_job()])
    flow.add_hosts_uuids("abc", prepend=True)
    assert flow[0].hosts == ["abc", flow.uuid]


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
    assert len(flow1) == 2
    assert add_job2.hosts == [flow1.uuid]

    with pytest.raises(ValueError, match="jobs array contains multiple jobs/flows"):
        flow1.add_jobs(add_job2)

    add_job3 = get_test_job()
    Flow(add_job3)

    # job belongs to another Flow
    with pytest.raises(ValueError, match="already belongs to another flow"):
        flow1.add_jobs(add_job3)

    add_job4 = get_test_job()
    with pytest.raises(ValueError, match="jobs array contains multiple jobs/flows"):
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
    with pytest.raises(
        ValueError, match="circular dependency: Flow .+ contains the current Flow"
    ):
        flow1.add_jobs(flow3)

    # test passing single job to @jobs setter
    flow1.jobs = add_job1
    assert len(flow1) == 1
    assert flow1[0] is add_job1


def test_remove_jobs():
    from jobflow.core.flow import Flow

    # test removing one job
    add_job1 = get_test_job()
    add_job2 = get_test_job()
    flow1 = Flow([add_job1, add_job2])

    flow1.remove_jobs(0)
    assert len(flow1) == 1
    assert flow1[0].uuid is add_job2.uuid

    with pytest.raises(ValueError, match="Only indices between 0 and the number of"):
        flow1.remove_jobs(-1)
    with pytest.raises(
        ValueError,
        match="Only indices between 0 and the number of the jobs are accepted",
    ):
        flow1.remove_jobs(10)

    # test removing two jobs
    add_job1 = get_test_job()
    add_job2 = get_test_job()
    add_job3 = get_test_job()
    flow = Flow([add_job1, add_job2, add_job3])

    flow.remove_jobs([0, 2])
    assert len(flow) == 1
    assert flow[0].uuid is add_job2.uuid

    # test removing job which is used in output
    add_job1 = get_test_job()
    add_job2 = get_test_job()
    flow2 = Flow([add_job1, add_job2], output=add_job2.output)

    with pytest.raises(
        ValueError, match="Removed Jobs/Flows are referenced in the output of the Flow"
    ):
        flow2.remove_jobs(1)

    # test removing a flow
    add_job1 = get_test_job()
    add_job2 = get_test_job()
    add_job3 = get_test_job()
    flow_inner = Flow([add_job1, add_job2])
    flow = Flow([flow_inner, add_job3])

    flow.remove_jobs(0)
    assert len(flow) == 1
    assert flow[0].uuid is add_job3.uuid

    # test removing a flow which is used in output
    add_job1 = get_test_job()
    add_job2 = get_test_job()
    add_job3 = get_test_job()
    flow_inner = Flow([add_job1, add_job2])
    flow = Flow([flow_inner, add_job3], output=flow_inner[0].output)

    with pytest.raises(
        ValueError, match="Removed Jobs/Flows are referenced in the output of the Flow"
    ):
        flow.remove_jobs(0)

    # test removing a job in a flow containing another flow
    add_job1 = get_test_job()
    add_job2 = get_test_job()
    add_job3 = get_test_job()
    flow_inner = Flow([add_job1, add_job2])
    flow = Flow([flow_inner, add_job3])

    flow.remove_jobs(1)
    assert len(flow) == 1
    assert flow[0].uuid is flow_inner.uuid


def test_set_output():
    from jobflow.core.flow import Flow

    add_job1 = get_test_job()
    add_job2 = get_test_job()
    add_job3 = get_test_job()
    flow = Flow([add_job1, add_job2], output=add_job2.output)

    flow.output = add_job1.output
    assert flow.output.uuid == add_job1.uuid

    with pytest.raises(
        ValueError, match="jobs array does not contain all jobs needed for flow output"
    ):
        flow.output = [add_job3.output]


def test_update_metadata():
    from jobflow import Flow, Job

    # test no filter
    flow = get_test_flow()
    flow.update_metadata({"b": 5})
    assert flow[0].metadata["b"] == 5
    assert flow[1].metadata["b"] == 5

    # test name filter
    flow = get_test_flow()
    flow.update_metadata({"b": 5}, name_filter="div")
    assert "b" not in flow[0].metadata
    assert flow[1].metadata["b"] == 5

    # test function filter
    flow = get_test_flow()
    flow.update_metadata({"b": 5}, function_filter=div)
    assert "b" not in flow[0].metadata
    assert flow[1].metadata["b"] == 5

    # test dict mod
    flow = get_test_flow()
    flow.update_metadata({"_inc": {"b": 5}}, function_filter=div, dict_mod=True)
    assert "b" not in flow[0].metadata
    assert flow[1].metadata["b"] == 8

    # test callback filter
    flow = get_test_flow()
    # Only update jobs with metadata containing "b"
    flow.update_metadata(
        {"c": 10}, callback_filter=lambda x: isinstance(x, Job) and "b" in x.metadata
    )
    assert "c" not in flow[0].metadata
    assert flow[1].metadata["c"] == 10
    assert "c" not in flow.metadata  # Flow itself shouldn't be updated

    # Test callback filter on Flow only
    flow = get_test_flow()
    flow.update_metadata(
        {"d": 15}, callback_filter=lambda x: isinstance(x, Flow) and x.name == "Flow"
    )
    assert flow.metadata["d"] == 15
    assert "d" not in flow[0].metadata
    assert "d" not in flow[1].metadata

    # Test callback filter with multiple conditions and nested structure
    from dataclasses import dataclass

    from jobflow import Maker, job

    @dataclass
    class TestMaker(Maker):
        name: str = "test_maker"

        @job
        def make(self):
            return Job(lambda: None, name="inner_job")

    maker = TestMaker()
    inner_flow = Flow([maker.make()], name="inner")
    outer_flow = Flow([inner_flow], name="outer")

    # Update only flows named "inner" and their jobs
    outer_flow.update_metadata(
        {"e": 20},
        callback_filter=lambda x: (isinstance(x, Flow) and x.name == "inner")
        or (isinstance(x, Job) and x.name == "inner_job"),
    )
    assert "e" not in outer_flow.metadata
    assert inner_flow.metadata["e"] == 20

    # Test callback filter with dynamic updates
    flow = get_test_flow()
    flow.update_metadata(
        {"f": 25},
        callback_filter=lambda x: isinstance(x, Job) and x.name.startswith("div"),
        dynamic=True,
    )
    assert "f" not in flow.metadata
    assert "f" not in flow[0].metadata
    assert flow[1].metadata["f"] == 25
    assert any(
        update.get("callback_filter") is not None for update in flow[1].metadata_updates
    )

    # Test callback filter with maker type checking
    flow = get_maker_flow()
    flow.update_metadata(
        {"g": 30},
        callback_filter=lambda x: (
            isinstance(x, Job) and x.maker is not None and x.maker.name == "div"
        ),
    )
    assert "g" not in flow.metadata
    assert "g" not in flow[0].metadata
    assert flow[1].metadata["g"] == 30


def test_flow_metadata_initialization():
    from jobflow import Flow

    # Test initialization with no metadata
    flow = Flow([])
    assert flow.metadata == {}

    # Test initialization with metadata
    metadata = {"key": "value"}
    flow = Flow([], metadata=metadata)
    # Test that metadata is the same object (not a copy, a reference)
    assert flow.metadata is metadata
    metadata["new_key"] = "new_value"
    assert flow.metadata["new_key"] == "new_value"

    # Test that modifying flow's metadata affects the original dictionary
    flow.metadata["flow_key"] = "flow_value"
    assert metadata["flow_key"] == "flow_value"


@pytest.mark.skip(reason="figure out how we want to implement excluding Flows/Jobs")
def test_flow_update_metadata():
    from jobflow import Flow, Job

    identity = lambda x: x  # noqa: E731
    job1 = Job(identity, name="job1")
    job2 = Job(identity, name="job2")
    flow = Flow([job1, job2], metadata={"initial": "value"})

    # Test updating only flow metadata
    flow.update_metadata({"flow_key": "flow_value"}, function_filter=Flow)
    assert flow.metadata == {"initial": "value", "flow_key": "flow_value"}
    assert "flow_key" not in job1.metadata
    assert "flow_key" not in job2.metadata

    # Test updating only jobs metadata
    flow.update_metadata({"job_key": "job_value"}, function_filter=job1)
    # assert "job_key" not in flow.metadata # TODO reinsert this assert once fix
    assert job1.metadata == {"job_key": "job_value"}
    assert job2.metadata == {"job_key": "job_value"}

    # Test updating both flow and jobs metadata
    flow.update_metadata({"both_key": "both_value"})
    assert flow.metadata == {
        "initial": "value",
        "flow_key": "flow_value",
        "both_key": "both_value",
    }
    assert job1.metadata == {"job_key": "job_value", "both_key": "both_value"}
    assert job2.metadata == {"job_key": "job_value", "both_key": "both_value"}


def test_flow_update_metadata_with_filters():
    from jobflow import Flow, Job

    job1 = Job(lambda x: x, name="job1")
    job2 = Job(lambda x: x, name="job2")
    flow = Flow([job1, job2])

    # Test name filter
    flow.update_metadata({"filtered": "value"}, name_filter="job1")
    assert "filtered" in job1.metadata
    assert "filtered" not in job2.metadata

    # Test function filter
    def filter_func(x):
        return x

    job3 = Job(filter_func, name="job3")
    flow.add_jobs(job3)
    flow.update_metadata({"func_filtered": "value"}, function_filter=filter_func)
    assert "func_filtered" in job3.metadata
    assert "func_filtered" not in job1.metadata
    assert "func_filtered" not in job2.metadata


def test_flow_update_metadata_dict_mod():
    from jobflow import Flow, Job

    identity = lambda x: x  # noqa: E731
    job = Job(identity, name="job", metadata={"count": 1})
    flow = Flow([job], metadata={"count": 1})

    # Test dict_mod on flow
    flow.update_metadata({"_inc": {"count": 1}}, dict_mod=True, function_filter=Flow)
    assert flow.metadata["count"] == 2
    assert job.metadata["count"] == 1, "job metadata count should not have been changed"

    # Test dict_mod on jobs
    flow.update_metadata(
        {"_inc": {"count": 1}}, dict_mod=True, function_filter=identity
    )
    assert flow.metadata["count"] == 3  # TODO fix this, expecting 2 actually
    assert job.metadata["count"] == 2


def test_flow_update_metadata_dynamic(memory_jobstore):
    from dataclasses import dataclass

    from jobflow import Flow, Job, Maker, Response, job

    @dataclass
    class TestMaker(Maker):
        name: str = "test_maker"

        @job
        def make(self):
            return Job(self.inner_job, name="dynamic_job")

        def inner_job(self):
            return Response()

    @job
    def use_maker(maker):
        return Response(replace=maker.make())

    maker = TestMaker()
    initial_job = use_maker(maker)
    flow = Flow([initial_job])

    # Test dynamic updates
    flow.update_metadata({"dynamic": "value"}, dynamic=True)

    # Run the flow to generate the dynamic job
    from jobflow.managers.local import run_locally

    run_locally(flow, store=memory_jobstore)

    # Check that the dynamic job has the metadata
    assert "dynamic" in flow[0].metadata
    assert flow[0].metadata["dynamic"] == "value"

    # Check that the metadata update is stored in the job's metadata_updates
    assert len(flow[0].metadata_updates) > 0
    assert any(
        update["update"].get("dynamic") == "value"
        for update in flow[0].metadata_updates
    )

    # Test nested flow
    @job
    def create_nested_flow(maker):
        nested_job = maker.make()
        return Response(replace=Flow([nested_job]))

    nested_initial_job = create_nested_flow(maker)
    outer_flow = Flow([nested_initial_job])

    outer_flow.update_metadata({"nested_dynamic": "nested_value"}, dynamic=True)

    run_locally(outer_flow, store=memory_jobstore)

    # Check that the nested dynamic job has the metadata
    assert "nested_dynamic" in outer_flow[0].metadata
    assert outer_flow[0].metadata["nested_dynamic"] == "nested_value"

    # Check that the metadata update is stored in the nested job's metadata_updates
    assert len(outer_flow[0].metadata_updates) > 0
    assert any(
        update["update"].get("nested_dynamic") == "nested_value"
        for update in outer_flow[0].metadata_updates
    )

    # Verify that the metadata was passed to the innermost job
    assert "nested_dynamic" in outer_flow[0].metadata
    assert outer_flow[0].metadata["nested_dynamic"] == "nested_value"

    # Test callback filter with dynamic updates
    @job
    def create_dynamic_flow(maker):
        nested_job = maker.make()
        nested_job.name = "dynamic_nested_job"  # Set specific name for testing
        return Response(replace=Flow([nested_job]))

    maker = TestMaker()
    initial_job = create_dynamic_flow(maker)
    flow = Flow([initial_job])

    # Update metadata only for jobs named "dynamic_nested_job"
    flow.update_metadata(
        {"dynamic_filtered": "filtered_value"},
        callback_filter=lambda x: isinstance(x, Job) and x.name == "dynamic_nested_job",
        dynamic=True,
    )

    run_locally(flow, store=memory_jobstore)

    # Original job shouldn't have the metadata
    assert "dynamic_filtered" not in flow[0].metadata

    # Get the replacement flow and check its job
    replacement_flow = flow[0].run(memory_jobstore).replace
    assert "dynamic_filtered" in replacement_flow[0].metadata
    assert replacement_flow[0].metadata["dynamic_filtered"] == "filtered_value"

    # Verify callback_filter was stored and propagated
    assert any(
        "callback_filter" in update and "dynamic_filtered" in update["update"]
        for update in replacement_flow[0].metadata_updates
    )

    # Test callback filter with nested dynamic updates
    nested_initial_job = create_dynamic_flow(maker)
    outer_flow = Flow([nested_initial_job])

    # Update metadata only for flows containing jobs with specific names
    outer_flow.update_metadata(
        {"nested_dynamic_filtered": "nested_filtered_value"},
        callback_filter=lambda x: (
            isinstance(x, Flow) and any(j.name == "dynamic_nested_job" for j in x)
        ),
        dynamic=True,
    )

    run_locally(outer_flow, store=memory_jobstore)

    # Check that the callback filter worked correctly
    replacement_flow = outer_flow[0].run(memory_jobstore).replace
    assert "nested_dynamic_filtered" in replacement_flow.metadata
    assert (
        replacement_flow.metadata["nested_dynamic_filtered"] == "nested_filtered_value"
    )
    assert "nested_dynamic_filtered" not in replacement_flow[0].metadata

    # Verify callback_filter was stored and propagated correctly
    assert any(
        "callback_filter" in update and "nested_dynamic_filtered" in update["update"]
        for update in replacement_flow.metadata_updates
    )


def test_flow_metadata_serialization():
    import json

    from monty.json import MontyDecoder, MontyEncoder

    from jobflow import Flow

    flow = Flow([], metadata={"key": "value"})
    encoded = json.dumps(flow, cls=MontyEncoder)
    decoded = json.loads(encoded, cls=MontyDecoder)

    assert decoded.metadata == flow.metadata


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
    assert flow[0].config == new_config
    assert flow[1].config == new_config

    # test name filter
    flow = get_test_flow()
    flow.update_config(new_config, name_filter="div")
    assert flow[0].config != new_config
    assert flow[1].config == new_config

    # test function filter
    flow = get_test_flow()
    flow.update_config(new_config, function_filter=div)
    assert flow[0].config != new_config
    assert flow[1].config == new_config

    # test attributes
    flow = get_test_flow()
    flow.update_config(new_config, function_filter=div, attributes=["manager_config"])
    assert flow[0].config.manager_config == {}
    assert flow[0].config.resolve_references
    assert flow[1].config.manager_config == {"a": "b"}
    assert flow[1].config.resolve_references


# def test_flow_magic_methods():
#     from jobflow import Flow
#
#     # prepare test jobs and flows
#     job1, job2, job3, job4, job5, job6 = map(get_test_job, range(6))
#
#     flow1 = Flow([job1])
#     flow2 = Flow([job2, job3])
#
#     # test __len__
#     assert len(flow1) == 1
#     assert len(flow2) == 2
#
#     # test __getitem__
#     assert flow2[0] == job2
#     assert flow2[1] == job3
#
#     # test __setitem__
#     flow2[0] = job4
#     assert flow2[0] == job4
#
#     # test __iter__
#     for job in flow2:
#         assert job in {job4, job3}
#
#     # test __contains__
#     assert job1 in flow1
#     assert job4 in flow2
#     assert job3 in flow2
#
#     # test __add__
#     flow3 = flow1 + job5
#     assert len(flow3) == 2
#     assert job5 in flow3
#
#     # test __sub__
#     flow4 = flow3 - job5
#     assert len(flow4) == 1 == len(flow1)
#     assert job5 not in flow4
#
#     # test __eq__ and __hash__
#     assert flow1 == flow1
#     assert flow1 != flow2
#     assert hash(flow1) != hash(flow2)
#
#     # test __getitem__ with out of range index
#     with pytest.raises(IndexError):
#         _ = flow1[10]
#
#     # test __setitem__ with out of range index
#     with pytest.raises(IndexError):
#         flow1[10] = job4
#
#     # test __contains__ with job not in flow
#     assert job5 not in flow1
#     assert flow2 not in flow1
#
#     # test __add__ with non-job item
#     with pytest.raises(TypeError):
#         _ = job6 + "not a job"
#
#     # test __sub__ with non-job item
#     with pytest.raises(TypeError):
#         _ = job6 - "not a job"
#
#     # test __sub__ with job not in flow
#     with pytest.raises(
#         ValueError, match=r"Job\(name='add', uuid='.+'\) not found in flow"
#     ):
#         _ = flow1 - job5
#
#     # test __eq__ with non-flow item
#     assert flow1 != "not a flow"


# def test_flow_magic_methods_edge_cases():
#     from jobflow import Flow
#
#     # prepare test jobs and flows
#     job1, job2, job3, job4, job5, job6 = map(get_test_job, range(6))
#     Flow([job6])
#     empty_flow = Flow([])
#     flow1 = Flow([job1, job2, job3, job4])
#
#     # test negative indexing with __getitem__ and __setitem__
#     assert flow1[-1] == job4
#     flow1[-1] = job5
#     assert flow1[-1] == job5
#
#     # test slicing with __getitem__ and __setitem__
#     assert flow1[1:3] == (job2, job3)
#     flow1[1] = job4  # test single item
#     assert flow1[1] == job4
#     flow1[1:3] = (job4, job5)  # test multiple items with slicing
#     assert flow1[1:3] == (job4, job5)
#
#     # test __add__ with bad type
#     assert flow1.__add__("string") == NotImplemented
#
#     for val in (None, 1.0, 1, "1", [1], (1,), {1: 1}):
#         type_name = type(val).__name__
#         with pytest.raises(
#             TypeError,
#             match=f"Flow can only contain Job or Flow objects, not {type_name}",
#         ):
#             flow1[1:3] = val
#
#     # adding an empty flow still increases len by 1
#     assert len(flow1 + empty_flow) == len(flow1) + 1
#
#     # test __add__ and __sub__ with job already in the flow
#     with pytest.raises(
#         ValueError, match="jobs array contains multiple jobs/flows with the same uuid"
#     ):
#         _ = flow1 + job1
#
#     with pytest.raises(ValueError, match="Job .+ already belongs to another flow"):
#         _ = flow1 + job6


def test_flow_repr():
    from jobflow import Flow

    # prepare jobs and flows
    job1, job2, job3, job4, job5, job6, job7 = map(get_test_job, range(7))

    flow1 = Flow([job1])
    flow2 = Flow([job2, job3])
    sub_flow1 = Flow([job6, job7])
    flow3 = Flow([job4, job5, sub_flow1])
    flow4 = Flow([flow1, flow2, flow3])

    flow_repr = repr(flow4).splitlines()

    lines = (
        "Flow(name='Flow', uuid='",
        "1. Flow(name='Flow', uuid='",
        "  1.1. Job(name='add', uuid='",
        "2. Flow(name='Flow', uuid='",
        "  2.1. Job(name='add', uuid='",
        "  2.2. Job(name='add', uuid='",
        "3. Flow(name='Flow', uuid='",
        "  3.1. Job(name='add', uuid='",
        "  3.2. Job(name='add', uuid='",
        "  3.3. Flow(name='Flow', uuid='",
        "    3.3.1. Job(name='add', uuid='",
        "    3.3.2. Job(name='add', uuid='",
    )

    assert len(lines) == len(flow_repr)
    for expected, line in zip(lines, flow_repr):
        assert line.startswith(expected), f"{line=} doesn't start with {expected=}"


def test_flow_decorator():
    from jobflow import flow, job, run_locally

    @job
    def add(a, b):
        return a + b

    @job
    def multiply(a, b):
        return a * b

    @flow
    def add_multiply(a, b, c):
        addition = add(a, b)
        return multiply(addition, c)

    f = add_multiply(1, 2, 3)
    outputs = run_locally(f)
    results = [v[1].output for k, v in outputs.items()]
    assert 3 in results
    assert 9 in results
