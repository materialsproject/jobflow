"""Tools for constructing Job and Flow graphs."""

from __future__ import annotations

import warnings

import networkx as nx
from monty.dev import requires

try:
    import matplotlib
except ImportError:
    matplotlib = None

import typing

if typing.TYPE_CHECKING:
    from typing import Tuple

    import jobflow

__all__ = ["itergraph", "draw_graph", "to_pydot"]


def itergraph(graph: nx.DiGraph):
    """
    Iterate through a graph using a topological sort order.

    This means the nodes are yielded such that for every directed edge (u v)
    node u comes before v in the ordering.

    Parameters
    ----------
    graph
        A networkx graph.

    Raises
    ------
    ValueError
        If the graph contains cycles.

    Yields
    ------
    str
        The node uuid.
    """
    if not nx.is_directed_acyclic_graph(graph):
        raise ValueError("Graph is not acyclic, cannot determine dependency order.")

    subgraphs = [graph.subgraph(c) for c in nx.weakly_connected_components(graph)]

    if len(subgraphs) > 1:
        warnings.warn("Some jobs are not connected, their ordering may be random")

    for subgraph in subgraphs:
        yield from nx.topological_sort(subgraph)


@requires(matplotlib, "matplotlib must be installed to plot flow graphs.")
def draw_graph(
    graph: nx.DiGraph,
    layout_function: typing.Callable = None,
    figsize: Tuple[float, float] = (12, 8),
):
    """
    Draw a networkx graph.

    Parameters
    ----------
    graph
        A graph object.
    layout_function
        A networkx layout function to use as the graph layout. For example,
        :obj:`.planar_layout`.
    figsize
        The figure size as a tuple of ``(width, height)``.

    Returns
    -------
    matplotlib.pyplot
        The matplotlib pyplot object.
    """
    import matplotlib.pyplot as plt

    if layout_function is None:
        try:
            pos = nx.nx_pydot.graphviz_layout(graph, prog="dot")
        except (ImportError, ModuleNotFoundError, FileNotFoundError):
            # graphviz not installed
            pos = nx.planar_layout(graph)
    else:
        pos = layout_function(graph)

    plt.figure(figsize=figsize)

    nodes = graph.nodes()
    labels = nx.get_node_attributes(graph, "label")

    nx.draw_networkx_edges(graph, pos)
    nx.draw_networkx_nodes(
        graph, pos, nodelist=nodes, node_color="#B65555", linewidths=1, edgecolors="k"
    )
    nx.draw_networkx_labels(graph, pos, labels=labels)

    edge_labels = nx.get_edge_attributes(graph, "properties")
    nx.draw_networkx_edge_labels(graph, pos, edge_labels=edge_labels, rotate=False)

    plt.gca().axis("off")

    return plt


def to_pydot(flow: jobflow.Flow):
    """
    Convert a flow to a pydot graph.

    Pydot graphs can be visualised using graphviz and support more advanced features
    than networkx graphs. For example, the pydot graph also includes the flow
    containers.

    .. Note::
        Requires pydot and graphviz to be installed.

    Parameters
    ----------
    flow
        A flow.

    Returns
    -------
    pydot.Dot
        The pydot graph.

    Examples
    --------
    The pydot graph can be generated from a flow using:

    >>> from jobflow import job, Flow
    >>> @job
    ... def add(a, b):
    ...     return a + b
    >>> add_first = add(1, 2)
    >>> add_second = add(add_first.output, 2)
    >>> my_flow = Flow(jobs=[add_first, add_second])
    >>> graph = to_pydot(my_flow)

    If graphviz is installed, the pydot graph can be rendered to a file using:

    >>> graph.write("output.png", format="png")
    """
    import pydot

    from jobflow import Flow

    nx_graph = flow.graph
    pydot_graph = pydot.Dot(f'"{flow.name}"', graph_type="digraph")

    for n, nodedata in nx_graph.nodes(data=True):
        str_nodedata = {k: str(v) for k, v in nodedata.items()}
        p = pydot.Node(str(n), **str_nodedata)
        pydot_graph.add_node(p)

    for u, v, edgedata in nx_graph.edges(data=True):
        str_edgedata = {k: str(v) for k, v in edgedata.items()}
        edge = pydot.Edge(str(u), str(v), label=str_edgedata["properties"])
        pydot_graph.add_edge(edge)

    def add_cluster(nested_flow, outer_graph):
        cluster = pydot.Cluster(nested_flow.uuid)
        cluster.set_label(nested_flow.name)
        for job in nested_flow.jobs:
            if isinstance(job, Flow):
                add_cluster(job, cluster)
            else:
                cluster.add_node(pydot_graph.get_node(f'"{job.uuid}"')[0])

        outer_graph.add_subgraph(cluster)

    add_cluster(flow, pydot_graph)

    return pydot_graph
