# -*- coding: utf-8 -*-
from operator import itemgetter

from kiara.api import KiaraModule
from kiara.models.values.value import ValueMap

KIARA_METADATA = {
    "authors": [
        {"name": "Caitlin Burge", "email": "caitlin.burge@uni.lu"},
    ],
    "description": "Kiara modules for: network_analysis",
}


class Degree_Ranking(KiaraModule):
    """Creates an ordered table with the rank and raw score for degree and weighted degree.
    Unweighted degree centrality uses an undirected graph and measures the number of independent connections each node has.
    Weighted degree centrality uses a directed graph and measures the total number of connections or weight attached to a node.

    Uses networkx degree.
    https://networkx.org/documentation/stable/reference/generated/networkx.classes.function.degree.html"""

    _module_type_name = "tropy.create.degree_rank_list"

    def create_inputs_schema(self):
        return {
            "network_graph": {
                "type": "network_graph",
                "doc": "The network graph being queried.",
            },
        }

    def create_outputs_schema(self):
        return {
            "centrality_network": {
                "type": "network_graph",
                "doc": "Updated network data with degree ranking assigned as a node attribute.",
            },
        }

    def process(self, inputs: ValueMap, outputs: ValueMap):

        import networkx as nx
        from kiara_plugin.tropy.models import NetworkGraph

        edges = inputs.get_value_obj("network_graph")

        network_data: NetworkGraph = edges.data

        G = network_data.as_networkx_graph()
        G.remove_edges_from(list(nx.selfloop_edges(G)))

        degree = {}
        for node in G:
            degree[node] = G.degree(node)
        nx.set_node_attributes(G, degree, "Degree")

        if nx.is_weighted(G, weight='weight') == True:
                weight_degree = {}
                for node in G:
                    weight_degree[node] = G.degree(node, weight="weight")
                nx.set_node_attributes(G, weight_degree, "Weighted Degree")
            

        attribute_network = NetworkGraph.create_from_networkx_graph(
            G,
            source_column_name=G.source_column_name,
            target_column_name=G.target_column_name,
            node_id_column_name=G.node_id_column_name,
        )

        outputs.set_values(centrality_network=attribute_network)


class Betweenness_Ranking(KiaraModule):
    """Creates an ordered table with the rank and raw score for betweenness centrality.
    Betweenness centrality measures the percentage of all shortest paths that a node appears on, therefore measuring the likeliness that a node may act as a connector or 'intermediary'.

    Uses a directed graph and networkx.betweenness_centrality()
    https://networkx.org/documentation/stable/reference/algorithms/generated/networkx.algorithms.centrality.betweenness_centrality.html#networkx.algorithms.centrality.betweenness_centrality"""

    _module_type_name = "tropy.create.betweenness_rank_list"

    def create_inputs_schema(self):
        return {
            "network_graph": {
                "type": "network_graph",
                "doc": "The network graph being queried.",
            },
            "weight_meaning": {
                "type": "boolean",
                "default": True,
                "doc": "How the weights given should be interpreted. If 'True', weight will be defined positively as 'strength', and these edges will be prioritised in shortest path calculations. If 'False', weight will be defined negatively as 'cost' or 'distance', and these edges will be avoided in shortest path calculations.",
            },
        }

    def create_outputs_schema(self):
        return {
            "centrality_network": {
                "type": "network_graph",
                "doc": "Updated network data with betweenness ranking assigned as a node attribute.",
            },
        }

    def process(self, inputs: ValueMap, outputs: ValueMap):

        import networkx as nx
        from kiara_plugin.tropy.models import NetworkGraph

        edges = inputs.get_value_obj("network_graph")
        wm = inputs.get_value_data("weight_meaning")

        network_data: NetworkGraph = edges.data

        G = network_data.as_networkx_graph()
        G.remove_edges_from(list(nx.selfloop_edges(G)))

        between = nx.betweenness_centrality(G)
        nx.set_node_attributes(G, between, "Betweenness Score")

        if nx.is_weighted(G, weight='weight') == True:
            weight_betweenness = nx.betweenness_centrality(G, weight="weight")
            nx.set_node_attributes(G, weight_betweenness, "Weighted Betweenness Score")

        attribute_network = NetworkGraph.create_from_networkx_graph(G)

        outputs.set_values(centrality_network=attribute_network)


class Eigenvector_Ranking(KiaraModule):
    """Creates an ordered table with the rank and raw score for betweenness centrality.
    Eigenvector centrality measures the extent to which a node is connected to other nodes of importance or influence.

    Uses an undirected graph networkx.eigenvector_centrality()
    https://networkx.org/documentation/stable/reference/algorithms/generated/networkx.algorithms.centrality.eigenvector_centrality.html#networkx.algorithms.centrality.eigenvector_centrality"""

    _module_type_name = "tropy.create.eigenvector_rank_list"

    def create_inputs_schema(self):
        return {
            "network_graph": {
                "type": "network_graph",
                "doc": "The network graph being queried.",
            },
            "iterations": {
                "type": "integer", 
                "default": 1000},
            "weight_meaning": {
                "type": "boolean",
                "default": True,
                "doc": "How the weights given should be interpreted. If 'True', weight will be defined positively as 'strength', and these edges will be prioritised in shortest path calculations. If 'False', weight will be defined negatively as 'cost' or 'distance', and these edges will be avoided in shortest path calculations.",
            },
        }

    def create_outputs_schema(self):
        return {
            "centrality_network": {
                "type": "network_graph",
                "doc": "Updated network data with eigenvector ranking assigned as a node attribute.",
            },
        }

    def process(self, inputs: ValueMap, outputs: ValueMap):

        import networkx as nx
        from kiara_plugin.tropy.models import NetworkGraph

        edges = inputs.get_value_obj("network_graph")
        iterations = inputs.get_value_data("iterations")
        wm = inputs.get_value_data("weight_meaning")

        network_data: NetworkGraph = edges.data

        G = network_data.as_networkx_graph()
        G.remove_edges_from(list(nx.selfloop_edges(G)))

        eigenvector = nx.eigenvector_centrality(G, max_iter=iterations)
        nx.set_node_attributes(G, eigenvector, "Eigenvector Score")

        if nx.is_weighted(G, weight='weight') == True:
            weight_eigenvector = nx.eigenvector_centrality(
                G, weight="weight", max_iter=100000
            )
            nx.set_node_attributes(G, weight_eigenvector, "Weighted Eigenvector Score")

        attribute_network = NetworkGraph.create_from_networkx_graph(G)

        outputs.set_values(centrality_network=attribute_network)


class Closeness_Ranking(KiaraModule):
    """Creates an ordered table with the rank and raw score for closeness centrality.
    Closeness centrality measures the average shortest distance path between a node and all reachable nodes in the network.

    Uses a directed graph and networkx.closeness_centrality()
    https://networkx.org/documentation/stable/reference/algorithms/generated/networkx.algorithms.centrality.closeness_centrality.html#networkx.algorithms.centrality.closeness_centrality"""

    _module_type_name = "tropy.create.closeness_rank_list"

    def create_inputs_schema(self):
        return {
            "network_graph": {
                "type": "network_graph",
                "doc": "The network graph being queried.",
            },
            "weight_meaning": {
                "type": "boolean",
                "default": True,
                "doc": "How the weights given should be interpreted. If 'True', weight will be defined positively as 'strength', and these edges will be prioritised in shortest path calculations. If 'False', weight will be defined negatively as 'cost' or 'distance', and these edges will be avoided in shortest path calculations.",
            },
        }

    def create_outputs_schema(self):
        return {
            "centrality_network": {
                "type": "network_graph",
                "doc": "Updated network data with closeness ranking assigned as a node attribute.",
            },
        }

    def process(self, inputs: ValueMap, outputs: ValueMap):

        import networkx as nx
        from kiara_plugin.tropy.models import NetworkGraph

        edges = inputs.get_value_obj("network_graph")
        wm = inputs.get_value_data("weight_meaning")

        network_data: NetworkGraph = edges.data

        G = network_data.as_networkx_graph()
        G.remove_edges_from(list(nx.selfloop_edges(G)))

        closeness = nx.closeness_centrality(G)
        nx.set_node_attributes(G, closeness, "Closeness Score")

        if nx.is_weighted(G, weight='weight') == True:
            weight_closeness = nx.closeness_centrality(G, weight="weight")
            nx.set_node_attributes(G, weight_closeness, "Weighted Closeness Score")

        attribute_network = NetworkGraph.create_from_networkx_graph(G)

        outputs.set_values(centrality_network=attribute_network)
