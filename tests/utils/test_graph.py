import importlib

import pytest


def test_itergraph():
    from networkx import DiGraph

    from jobflow.utils.graph import itergraph

    # test linear
    graph = DiGraph([("a", "b"), ("b", "c")])
    result = list(itergraph(graph))
    assert result == ["a", "b", "c"]

    # test branched
    graph = DiGraph([("a", "b"), ("b", "c"), ("a", "c"), ("d", "b")])
    result = list(itergraph(graph))
    assert result == ["a", "d", "b", "c"] or result == ["d", "a", "b", "c"]

    # test non-connected
    graph = DiGraph([("a", "b"), ("c", "d")])
    with pytest.warns(UserWarning):
        result = list(itergraph(graph))
    assert (
        result == ["a", "b", "c", "d"]
        or result == ["c", "d", "a", "b"]
        or result == ["a", "c", "b", "d"]
        or result == ["a", "c", "d", "b"]
        or result == ["c", "a", "b", "d"]
        or result == ["c", "a", "d", "b"]
    )

    # test non DAG
    graph = DiGraph([("a", "b"), ("b", "a")])
    with pytest.raises(ValueError):
        list(itergraph(graph))


def test_draw_graph():
    from networkx import DiGraph, planar_layout

    from jobflow.utils.graph import draw_graph

    graph = DiGraph([("a", "b", {"properties": "1"}), ("b", "c", {"properties": "x"})])
    assert draw_graph(graph)

    assert draw_graph(graph, layout_function=planar_layout)


@pytest.mark.usefixtures("no_pydot")
def test_draw_graph_no_pydot():
    from networkx import DiGraph

    from jobflow.utils.graph import draw_graph

    graph = DiGraph([("a", "b", {"properties": "1"}), ("b", "c", {"properties": "x"})])
    assert draw_graph(graph)


@pytest.mark.usefixtures("no_matplotlib")
def test_draw_graph_no_matplotlib():
    from networkx import DiGraph

    import jobflow.utils.graph

    importlib.reload(jobflow.utils.graph)

    graph = DiGraph([("a", "b", {"properties": "1"}), ("b", "c", {"properties": "x"})])
    with pytest.raises(RuntimeError):
        assert jobflow.utils.graph.draw_graph(graph)


def add(a, b):
    return a + b


def test_to_pydot():
    from jobflow import Flow, Job
    from jobflow.utils.graph import to_pydot

    # test edges
    add_job1 = Job(add, function_args=(1, 2))
    add_job2 = Job(add, function_args=(1, add_job1.output))
    flow = Flow([add_job1, add_job2])

    pydot = to_pydot(flow)
    assert pydot is not None

    # test nested
    add_job1 = Job(add, function_args=(1, 2))
    add_job2 = Job(add, function_args=(1, 2))
    add_job3 = Job(add, function_args=(1, 2))
    add_job4 = Job(add, function_args=(1, 2))
    flow1 = Flow([add_job1, add_job2])
    flow2 = Flow([add_job3, add_job4])
    main_flow = Flow([flow1, flow2])

    pydot = to_pydot(main_flow)
    assert pydot is not None
