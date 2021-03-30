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


def draw_graph(graph: nx.DiGraph, path=None, layout_function=nx.spring_layout):
    import matplotlib.pyplot as plt

    pos = layout_function(graph)
    plt.figure(figsize=(8, 8))

    nodes = graph.nodes()
    node_types = nx.get_node_attributes(graph, "type")
    colors = ["#5571AB" if node_types["n"] == "activity" else "#B65555" for n in nodes]
    labels = nx.get_node_attributes(graph, "label")

    nx.draw_networkx_edges(graph, pos)
    nx.draw_networkx_nodes(
        graph, pos, nodelist=nodes, node_color=colors, linewidths=1, edgecolors="k"
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

    return plt
