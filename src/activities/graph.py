"""Tools for constructing Task and Activity graphs."""
from typing import Union
from uuid import uuid4

from activities.activity import Activity
from activities.task import Task


def activity_input_graph(activity: Activity):
    import networkx as nx

    nodes = [(activity.uuid, {"type": "activity"})]
    edges = []

    for uuid, refs in activity.input_references_grouped.items():
        properties = list(set([ref.name for ref in refs]))
        edges.append((uuid, activity.uuid, {"properties": properties}))

    graph = nx.DiGraph()
    graph.add_nodes_from(nodes)
    graph.add_edges_from(edges)
    return graph


def activity_output_graph(activity: Activity):
    # add references from the tasks to the activity
    import networkx as nx
    nodes = [(activity.uuid, {"type": "activity"})]
    edges = []

    for uuid, refs in activity.output_references_grouped.items():
        properties = list(set([ref.name for ref in refs]))
        edges.append((uuid, activity.uuid, {"properties": properties}))

    graph = nx.DiGraph()
    graph.add_nodes_from(nodes)
    graph.add_edges_from(edges)
    return graph


def task_graph(activity: Activity):
    """
    Get a task graph from an ``Activity``.

    Parameters
    ----------
    activity
        An activity object.

    Returns
    -------
    A networkx Graph.
    """
    import networkx as nx
    from networkx.utils import pairwise

    if activity.contains_activities:
        raise ValueError("Activity must contain tasks not activities.")

    nodes = [(activity.uuid, {"type": "activity"})]
    edges = []

    # tasks have strictly linear edges but reference can occur between any
    # two nodes. First add linear edges
    for task_a, task_b in pairwise(activity.tasks):
        edges.append((task_a.uuid, task_b.uuid))

    # add edge between last task and the activity
    edges.append((activity.tasks[-1].uuid, activity.uuid))

    # next add input references
    for task in activity.tasks:
        nodes.append((task.uuid, {"type": "task"}))
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


def draw_graph(graph, name_mapping=None):
    import matplotlib.pyplot as plt
    import networkx as nx
    if name_mapping is not None:
        graph = nx.relabel_nodes(graph, name_mapping)

    # pos = nx.circular_layout(graph)
    # pos = nx.kamada_kawai_layout(graph)
    # pos = nx.planar_layout(graph)
    pos = nx.spring_layout(graph, k=10, iterations=100)
    from networkx.drawing.nx_pydot import graphviz_layout
    pos = graphviz_layout(graph, prog="dot")
    plt.figure(figsize=(16, 16))
    # nx.draw(graph, pos, with_labels=True)
    # node_labels = nx.get_node_attributes(graph, 'host')
    # nx.draw_networkx_labels(graph, pos, labels=node_labels)
    nodes = graph.nodes()
    node_types = nx.get_node_attributes(graph, 'type')
    colors = ["#5571AB" if node_types[n] == "activity" else "#B65555" for n in nodes]

    nx.draw_networkx_edges(graph, pos)
    nx.draw_networkx_nodes(graph, pos, nodelist=nodes, node_color=colors, linewidths=1, edgecolors="k")
    nx.draw_networkx_labels(graph, pos)

    edge_labels = nx.get_edge_attributes(graph, 'properties')
    nx.draw_networkx_edge_labels(graph, pos, edge_labels=edge_labels, rotate=False)

    # nodes = g.nodes()
    # colors = [mapping[g.node[n]['group']] for n in nodes]

    # drawing nodes and edges separately so we can capture collection for colobar
    # pos = nx.spring_layout(g)
    # ec = nx.draw_networkx_edges(g, pos, alpha=0.2)
    # nc = nx.draw_networkx_nodes(g, pos, nodelist=nodes, node_color=colors,
    #                             with_labels=False, node_size=100, cmap=plt.cm.j
    plt.show()

