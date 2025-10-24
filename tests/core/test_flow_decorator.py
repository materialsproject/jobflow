def test_flow_decorator_basic():
    """Test basic flow decorator usage."""
    from jobflow import flow
    from jobflow.core.flow import DecoratedFlow

    @flow
    def my_flow(a, b):
        return a + b

    # Test that decorator returns a function
    assert callable(my_flow)

    # Test that calling the decorated function returns a DecoratedFlow
    result = my_flow(1, 2)

    assert isinstance(result, DecoratedFlow)


def test_decorated_flow_attributes():
    """Test that DecoratedFlow stores function and arguments correctly."""
    from jobflow import flow

    def sample_func(x, y, z=10):
        return x + y + z

    decorated = flow(sample_func)
    result = decorated(5, 10, z=20)

    # Check that the function, args, and kwargs are stored
    assert result.fn == sample_func
    assert result.args == (5, 10)
    assert result.kwargs == {"z": 20}


def test_decorated_flow_initialization():
    """Test that DecoratedFlow initializes with empty jobs."""
    from jobflow import flow

    @flow
    def my_flow(a, b):
        return a + b

    result = my_flow(1, 2)

    # DecoratedFlow should start with empty jobs
    assert len(result.jobs) == 0


def test_flow_build_context():
    """Test flow_build_context context manager."""
    from jobflow import Flow
    from jobflow.core.flow import _current_flow_context, flow_build_context

    test_flow = Flow([], name="test")

    assert _current_flow_context.get() is None

    with flow_build_context(test_flow):
        assert _current_flow_context.get() is test_flow

    assert _current_flow_context.get() is None


def test_flow_build_context_nested():
    """Test nested flow_build_context contexts."""
    from jobflow import Flow
    from jobflow.core.flow import _current_flow_context, flow_build_context

    flow1 = Flow([], name="flow1")
    flow2 = Flow([], name="flow2")

    with flow_build_context(flow1):
        assert _current_flow_context.get() is flow1

        with flow_build_context(flow2):
            assert _current_flow_context.get() is flow2

        assert _current_flow_context.get() is flow1

    assert _current_flow_context.get() is None


def test_decorated_flow_multiple_calls():
    """
    Test that multiple calls to decorated function create different
    DecoratedFlows.
    """
    from jobflow import flow

    @flow
    def my_flow(x):
        return x * 2

    flow1 = my_flow(5)
    flow2 = my_flow(5)

    # Should be different instances
    assert flow1 is not flow2
    assert flow1.uuid != flow2.uuid


def test_flow_returns_none():
    """Test that a flow that does nothing can be run locally and returns the
    correct output."""
    from jobflow import flow
    from jobflow.managers.local import run_locally

    flow_did_run = False

    @flow
    def my_flow(a, b):
        nonlocal flow_did_run
        flow_did_run = True

    flow1 = my_flow(3, 4)
    result = run_locally(flow1, ensure_success=True)
    assert flow_did_run
    assert result is None


def test_flow_returns_job():
    """Test that a flow that returns a Job can be run locally and returns the
    correct output."""
    from jobflow import flow, job
    from jobflow.managers.local import run_locally

    @job
    def add(x, y):
        return x + y

    @flow
    def my_flow(a, b):
        return add(a, b)

    flow1 = my_flow(3, 4)
    result = run_locally(flow1, ensure_success=True)
    assert result == 7


def test_flow_returns_output_reference():
    """Test that a flow that returns an OutputReference can be run locally and
    returns the correct output."""
    from jobflow import flow, job
    from jobflow.managers.local import run_locally

    @job
    def add(x, y):
        return x + y

    @flow
    def my_flow(a, b):
        return add(a, b).output

    flow1 = my_flow(3, 4)
    result = run_locally(flow1, ensure_success=True)
    assert result == 7


def test_flow_returns_primitive():
    """Test that a flow that returns a primitive value can be run locally and
    returns the correct output."""
    from jobflow import flow, job
    from jobflow.managers.local import run_locally

    job_did_run = False

    @job
    def add(x, y):
        nonlocal job_did_run
        job_did_run = True
        return x + y

    @flow
    def my_flow(a, b):
        # Run a job for side effects, but return a primitive value
        _ = add(a, b)
        return 42

    flow1 = my_flow(3, 4)
    result = run_locally(flow1, ensure_success=True)
    assert job_did_run
    assert result == 42


def test_flow_returns_nested():
    """Test that a flow that returns a nested structure of mixed types can be
    run locally and returns the correct output."""
    from jobflow import flow, job
    from jobflow.managers.local import run_locally

    @job
    def add(x, y):
        return x + y

    @job
    def subtract(x, y):
        return x - y

    @flow
    def my_flow(a, b):
        return [add(a, b), add(b, a).output], {
            "foo": add(a, b).output,
            "bar": [add(a, b), subtract(a, b)],
        }

    flow1 = my_flow(3, 4)
    result = run_locally(flow1, ensure_success=True)
    assert result == tuple([[7, 7], {"foo": 7, "bar": [7, -1]}])  # noqa: C409


def test_replace_job_run_locally():
    """Test that a flow where a job is replaced can be run locally and returns
    the correct output."""
    from jobflow import Response, flow, job
    from jobflow.managers.local import run_locally

    @job
    def add(x, y):
        return x + y

    @job
    def add_again(x, y):
        another_job = add(x, y)
        return Response(replace=another_job)

    @flow
    def some_flow(a, b):
        x = add(a, b)  # a + b
        return add_again(x.output, b)  # a + b + b

    some_flow_run = some_flow(3, 4)
    result = run_locally(some_flow_run, ensure_success=True)
    assert result == 11  # 3 + 4 + 4


def test_dynamic_flow_run_locally():
    """Test that a flow where a job is replaced by several Flow objects can be
    run locally and returns the correct output."""
    from jobflow import Flow, Response, flow, job
    from jobflow.managers.local import run_locally

    @job
    def make_list_of_3(a):
        return [a] * 3

    @job
    def add(a, b):
        return a + b

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

    result = run_locally(add_distributed_flow(2), ensure_success=True)
    assert result == [3, 3, 3]
