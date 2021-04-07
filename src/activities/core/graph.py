"""Tools for constructing Job and Activity graphs."""
import warnings

import networkx as nx


def itergraph(graph: nx.DiGraph):
    if not nx.is_directed_acyclic_graph(graph):
        raise ValueError("Graph is not acyclic, cannot determine dependency order.")

    subgraphs = [graph.subgraph(c) for c in nx.weakly_connected_components(graph)]

    if len(subgraphs) > 1:
        warnings.warn("Some activities are not connected, their ordering may be random")

    for subgraph in subgraphs:
        for node in nx.topological_sort(subgraph):
            yield node


def draw_graph(graph: nx.DiGraph, path=None, layout_function=None):
    import matplotlib.pyplot as plt

    if layout_function is None:
        try:
            pos = nx.nx_pydot.graphviz_layout(graph, prog="dot")
        except:
            pos = nx.planar_layout(graph)
    else:
        pos = layout_function(graph)

    plt.figure(figsize=(12, 8))

    nodes = graph.nodes()
    labels = nx.get_node_attributes(graph, "label")

    nx.draw_networkx_edges(graph, pos)
    nx.draw_networkx_nodes(
        graph, pos, nodelist=nodes, node_color="#B65555", linewidths=1, edgecolors="k"
    )
    nx.draw_networkx_labels(graph, pos, labels=labels)

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

    plt.tight_layout()

    return plt


def to_pydot(activity):
    try:
        import pydot
    except ImportError:
        raise ImportError("pydot must be installed to use to_pydot.")

    from activities import Activity

    nx_graph = activity.graph
    pydot_graph = pydot.Dot(f'"{activity.name}"', graph_type="digraph")

    for n, nodedata in nx_graph.nodes(data=True):
        str_nodedata = {k: str(v) for k, v in nodedata.items()}
        p = pydot.Node(str(n), **str_nodedata)
        pydot_graph.add_node(p)

    for u, v, edgedata in nx_graph.edges(data=True):
        str_edgedata = {k: str(v) for k, v in edgedata.items()}
        edge = pydot.Edge(str(u), str(v), **str_edgedata)
        pydot_graph.add_edge(edge)

    def add_cluster(nested_activity, outer_graph):
        cluster = pydot.Cluster(str(nested_activity.uuid))
        cluster.set_label(nested_activity.name)
        for job in nested_activity.jobs:
            for sub_node in job.graph.nodes:
                cluster.add_node(pydot_graph.get_node(f'"{str(sub_node)}"')[0])
            if isinstance(job, Activity):
                add_cluster(job, cluster)
        outer_graph.add_subgraph(cluster)

    add_cluster(activity, pydot_graph)

    return pydot_graph
