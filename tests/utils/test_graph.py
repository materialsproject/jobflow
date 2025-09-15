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
    assert result in (["a", "d", "b", "c"], ["d", "a", "b", "c"])

    # test non-connected
    graph = DiGraph([("a", "b"), ("c", "d")])
    with pytest.warns(UserWarning, match="Some jobs are not connected"):
        result = list(itergraph(graph))
    assert {*result} == {"a", "b", "c", "d"}

    # test non DAG
    graph = DiGraph([("a", "b"), ("b", "a")])
    with pytest.raises(
        ValueError, match="Graph is not acyclic, cannot determine dependency order"
    ):
        list(itergraph(graph))


def test_draw_graph():
    pytest.importorskip("matplotlib")

    from networkx import DiGraph, planar_layout

    from jobflow.utils.graph import draw_graph

    graph = DiGraph([("a", "b", {"properties": "1"}), ("b", "c", {"properties": "x"})])
    assert draw_graph(graph)

    assert draw_graph(graph, layout_function=planar_layout)


def test_draw_graph_no_pydot(no_pydot):
    pytest.importorskip("matplotlib")

    from networkx import DiGraph

    from jobflow.utils.graph import draw_graph

    graph = DiGraph([("a", "b", {"properties": "1"}), ("b", "c", {"properties": "x"})])
    assert draw_graph(graph)


def test_draw_graph_no_matplotlib(no_matplotlib):
    from networkx import DiGraph

    import jobflow.utils.graph

    importlib.reload(jobflow.utils.graph)

    graph = DiGraph([("a", "b", {"properties": "1"}), ("b", "c", {"properties": "x"})])
    with pytest.raises(RuntimeError):
        assert jobflow.utils.graph.draw_graph(graph)


def add(a, b):
    return a + b


def test_to_pydot():
    pytest.importorskip("pydot")

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


def test_to_mermaid():
    from jobflow import Flow, Job
    from jobflow.utils.graph import to_mermaid

    # test edges
    add_job1 = Job(add, function_args=(1, 2))
    add_job2 = Job(add, function_args=(1, add_job1.output))
    flow = Flow([add_job1, add_job2])

    # test flow
    mermaid = to_mermaid(flow)
    assert mermaid is not None

    # test job
    add_job1 = Job(add, function_args=(1, 2))
    mermaid = to_mermaid(add_job1)
    assert mermaid is not None

    # test list of properties
    add_job1 = Job(add, function_args=(1, 2))
    add_job2 = Job(add, function_args=(add_job1.output.prop1, add_job1.output.prop2))
    flow = Flow([add_job1, add_job2])

    mermaid = to_mermaid(flow)
    assert ("prop1, prop2" in mermaid) or ("prop2, prop1" in mermaid)

    # test nested
    add_job1 = Job(add, function_args=(1, 2))
    add_job2 = Job(add, function_args=(1, 2))
    add_job3 = Job(add, function_args=(1, 2))
    add_job4 = Job(add, function_args=(1, 2))
    flow1 = Flow([add_job1, add_job2])
    flow2 = Flow([add_job3, add_job4])
    main_flow = Flow([flow1, flow2])

    mermaid = to_mermaid(main_flow, show_flow_boxes=True)
    assert "subgraph" in mermaid

    # test without flow boxes
    mermaid = to_mermaid(main_flow, show_flow_boxes=False)
    assert "subgraph" not in mermaid
