"""Tools for constructing Task and Activity graphs."""
import warnings
from typing import Union
from uuid import uuid4

import networkx as nx

from activities.core.activity import Activity
from activities.core.task import Task


def activity_input_graph(activity: Activity) -> nx.DiGraph:
    nodes = [(activity.uuid, {"type": "activity", "object": activity})]
    edges = []

    for uuid, refs in activity.input_references_grouped.items():
        properties = list(set([ref.name for ref in refs]))
        edges.append((uuid, activity.uuid, {"properties": properties}))

    graph = nx.DiGraph()
    graph.add_nodes_from(nodes)
    graph.add_edges_from(edges)
    return graph


def activity_output_graph(activity: Activity) -> nx.DiGraph:
    # add references from the tasks to the activity
    nodes = [(activity.uuid, {"type": "activity", "object": activity})]
    edges = []

    for uuid, refs in activity.output_references_grouped.items():
        properties = list(set([ref.name for ref in refs]))
        edges.append((uuid, activity.uuid, {"properties": properties}))

    graph = nx.DiGraph()
    graph.add_nodes_from(nodes)
    graph.add_edges_from(edges)
    return graph


def task_graph(activity: Activity) -> nx.DiGraph:
    """
    Get a task graph from an ``Activity``.

    Parameters
    ----------
    activity
        An activity object containing ``Task``s.

    Raises
    ------
    ValueError
        If the activity contains ``Activity``s.

    Returns
    -------
    A networkx Graph.
    """
    from networkx.utils import pairwise

    if activity.contains_activities:
        raise ValueError("Activity must contain tasks not activities.")

    nodes = [(activity.uuid, {"type": "activity", "object": activity})]
    edges = []

    # tasks have strictly linear edges but reference can occur between any
    # two nodes. First add linear edges
    for task_a, task_b in pairwise(activity.tasks):
        edges.append((task_a.uuid, task_b.uuid))

    # add edge between last task and the activity
    edges.append((activity.tasks[-1].uuid, activity.uuid))

    # next add input references
    for task in activity.tasks:
        nodes.append((task.uuid, {"type": "task", "object": task}))
        for uuid, refs in task.input_references_grouped.items():
            properties = list(set([ref.name for ref in refs]))
            edges.append((uuid, task.uuid, {"properties": properties}))

    # finally add references from the tasks to the activity
    for uuid, refs in activity.output_references_grouped.items():
        properties = list(set([ref.name for ref in refs]))
        edges.append((uuid, activity.uuid, {"properties": properties}))

    graph = nx.DiGraph()
    graph.add_nodes_from(nodes)
    graph.add_edges_from(edges)

    return graph


def activity_and_task_names(activity: Activity):
    mapping = {}
    used_names = set()

    def get_name(_activity: Union[Activity, Task]):
        if isinstance(_activity, Activity):
            for _subactivity in _activity.tasks:
                get_name(_subactivity)

            name = _activity.name
        else:
            name = _activity.function[1]
        if name in used_names:
            name += str(uuid4()).split("-")[0]

        mapping[_activity.uuid] = name
        used_names.add(name)

    get_name(activity)
    return mapping


def itergraph(graph: nx.DiGraph):
    if not nx.is_directed_acyclic_graph(graph):
        raise ValueError("Graph is not acyclic, cannot determine dependency order.")

    subgraphs = [graph.subgraph(c) for c in nx.weakly_connected_components(graph)]

    if len(subgraphs) > 1:
        warnings.warn("Some activities are not connected, their ordering may be random")

    for subgraph in subgraphs:
        for node in nx.topological_sort(subgraph):
            yield node


def draw_graph(graph: nx.DiGraph, name_mapping=None, path=None):
    import matplotlib.pyplot as plt

    if name_mapping is not None:
        graph = nx.relabel_nodes(graph, name_mapping)
        if path is not None:
            path = [name_mapping.get(p, p) for p in path]

    # pos = nx.circular_layout(graph)
    # pos = nx.kamada_kawai_layout(graph)
    # pos = nx.planar_layout(graph)
    pos = nx.spring_layout(graph, k=10, iterations=100)
    from networkx.drawing.nx_pydot import graphviz_layout

    pos = graphviz_layout(graph, prog="dot")
    plt.figure(figsize=(16, 16))

    nodes = graph.nodes()
    node_types = nx.get_node_attributes(graph, "type")
    colors = [
        "#5571AB" if node_types.get(n, "activity") == "activity" else "#B65555"
        for n in nodes
    ]

    nx.draw_networkx_edges(graph, pos)
    nx.draw_networkx_nodes(
        graph, pos, nodelist=nodes, node_color=colors, linewidths=1, edgecolors="k"
    )
    nx.draw_networkx_labels(graph, pos)

    edge_labels = nx.get_edge_attributes(graph, "properties")
    nx.draw_networkx_edge_labels(graph, pos, edge_labels=edge_labels, rotate=False)

    if path:
        import numpy as np
        from matplotlib import cm

        path_edges = list(zip(path, path[1:]))
        cmap = cm.get_cmap("viridis")
        colors = cmap(np.linspace(0, 1, len(path)))

        nx.draw_networkx_nodes(graph, pos, nodelist=path, node_color=colors)
        nx.draw_networkx_edges(
            graph, pos, edgelist=path_edges, edge_color=colors, width=2
        )

    return plt
