from jobflow.core.job import job


@job
def add(a, b):
    return a + b


def test_flow_decorator_basic():
    """Test basic flow decorator usage."""
    from jobflow import flow
    from jobflow.core.flow import DecoratedFlow

    @flow
    def my_flow(a, b):
        return add(a, b)

    # Test that decorator returns a function
    assert callable(my_flow)

    # Test that calling the decorated function returns a DecoratedFlow
    result = my_flow(1, 2)

    assert isinstance(result, DecoratedFlow)


def test_decorated_flow_attributes():
    """Test that DecoratedFlow stores function and arguments correctly."""
    from jobflow import flow

    def sample_func(x, y, z=10):
        return add(x, z)  # ignore y

    decorated = flow(sample_func)
    result = decorated(5, 10, z=20)

    # Check that the function, args, and kwargs are stored
    assert result.fn == sample_func
    assert result.args == (5, 10)
    assert result.kwargs == {"z": 20}


def test_decorated_flow_initialization():
    """Test that DecoratedFlow initializes with a job."""
    from jobflow import flow

    @flow
    def my_flow(a, b):
        return add(a, b)

    result = my_flow(1, 2)

    # DecoratedFlow should start with a single job
    assert len(result.jobs) == 1


def test_flow_build_context():
    """Test flow_build_context context manager."""
    from jobflow import Flow
    from jobflow.core.flow import _current_flow_context, flow_build_context

    test_flow = Flow([], name="test")

    assert _current_flow_context.get() is None

    with flow_build_context([test_flow]):
        assert test_flow in _current_flow_context.get()

    assert _current_flow_context.get() is None


def test_flow_build_context_nested():
    """Test nested flow_build_context contexts."""
    from jobflow import Flow
    from jobflow.core.flow import _current_flow_context, flow_build_context

    flow1 = Flow([], name="flow1")
    flow2 = Flow([], name="flow2")

    with flow_build_context([flow1]):
        assert _current_flow_context.get() == [flow1]

        with flow_build_context([flow2]):
            assert _current_flow_context.get() == [flow2]

        assert _current_flow_context.get() == [flow1]

    assert _current_flow_context.get() is None


def test_decorated_flow_multiple_calls():
    """
    Test that multiple calls to decorated function create different
    DecoratedFlows.
    """
    from jobflow import flow

    @flow
    def my_flow(x):
        return add(x, 1)

    flow1 = my_flow(5)
    flow2 = my_flow(5)

    # Should be different instances
    assert flow1 is not flow2
    assert flow1.uuid != flow2.uuid


def test_flow_returns_job():
    """Test that a flow that returns a Job can be run locally and returns the
    correct output."""
    from jobflow import flow
    from jobflow.managers.local import run_locally

    @flow
    def my_flow(a, b):
        return add(a, b)

    flow1 = my_flow(3, 4)
    result = run_locally(flow1, ensure_success=True)
    assert result[flow1.output.uuid][1].output == 7


def test_flow_returns_flow():
    """Test that a flow that returns a Flow can be run locally and returns the
    correct output."""
    from jobflow import Flow, flow
    from jobflow.managers.local import run_locally

    @flow
    def add_single(a, b):
        j1 = add(a, b)
        return add(j1.output, 2).output

    @flow
    def add_combine(a, b):
        j = add(a, b)
        f1 = Flow(j, j.output)
        return add_single(f1.output, 3)

    flow1 = add_combine(1, 2)
    result = run_locally(flow1, ensure_success=True)
    assert result[flow1.output.uuid][1].output == 8


def test_flow_returns_output_reference():
    """Test that a flow that returns an OutputReference can be run locally and
    returns the correct output."""
    from jobflow import flow
    from jobflow.managers.local import run_locally

    @flow
    def my_flow(a, b):
        return add(a, b).output

    flow1 = my_flow(3, 4)
    result = run_locally(flow1, ensure_success=True)
    assert result[flow1.output.uuid][1].output == 7


def test_flow_returns_list():
    """Test that a flow that returns a list of OutputReferences
    can be created and run."""
    from jobflow import flow
    from jobflow.managers.local import run_locally

    @flow
    def my_flow(a, b):
        return [add(a, a), add(b, b)]

    f = my_flow(1, 2)
    _ = run_locally(f, ensure_success=True)


def test_flow_nested():
    """Test that nested flow decorators work correctly."""
    from jobflow import flow
    from jobflow.managers.local import run_locally

    @flow
    def add_single(a, b):
        j1 = add(a, b)
        return add(j1.output, 2).output

    @flow
    def add_combine(a, b):
        f1 = add_single(a, b)
        return add_single(f1.output, 3).output

    f = add_combine(1, 2)
    results = run_locally(f, ensure_success=True)

    # Ensure all expected results (3, 5, 8, 10) are in the results.
    all_responses = {
        response.output
        for index_to_response in results.values()
        for response in index_to_response.values()
    }
    assert all_responses == {3, 5, 8, 10}


def test_replace_job_run_locally():
    """Test that a flow where a job is replaced can be run locally and returns
    the correct output."""
    from jobflow import Response, flow, job
    from jobflow.managers.local import run_locally

    @job
    def add_again(x, y):
        another_job = add(x, y)
        return Response(replace=another_job)

    @flow
    def some_flow(a, b):
        x = add(a, b)  # a + b
        return add_again(x.output, b)  # a + b + b

    some_flow_run = some_flow(3, 4)
    results = run_locally(some_flow_run, ensure_success=True)

    # Ensure the final result (3 + 4 + 4) is in the results.
    all_responses = [
        response.output
        for index_to_response in results.values()
        for response in index_to_response.values()
    ]
    assert 11 in all_responses


def test_dynamic_flow_run_locally():
    """Test that a flow where a job is replaced by several Flow objects can be
    run locally and returns the correct output."""
    from jobflow import Flow, Response, flow, job
    from jobflow.managers.local import run_locally

    @job
    def make_list_of_3(a):
        return [a] * 3

    @job
    def add_distributed(list_a):
        jobs = [add(val, 1) for val in list_a]
        flow = Flow(jobs)
        return Response(replace=flow)

    @flow
    def add_distributed_flow(a):
        job1 = make_list_of_3(a)
        job2 = add_distributed(job1.output)
        return job2.output

    results = run_locally(add_distributed_flow(2), ensure_success=True)

    # Ensure the final result (3 instances of 3s) is in the results.
    all_responses = [
        response.output
        for index_to_response in results.values()
        for response in index_to_response.values()
    ]
    assert all_responses.count(3) == 3


def test_decorate_maker():
    from dataclasses import dataclass

    from jobflow import Maker, flow
    from jobflow.managers.local import run_locally

    @dataclass
    class TestMaker(Maker):
        a: int
        name: str = "test_maker"

        @flow
        def make(self, b):
            j = add(self.a, b)
            return j.output

    f = TestMaker(a=1).make(2)

    assert f.name == "test_maker"

    results = run_locally(f, ensure_success=True)

    all_responses = [
        response.output
        for index_to_response in results.values()
        for response in index_to_response.values()
    ]
    assert 3 in all_responses
