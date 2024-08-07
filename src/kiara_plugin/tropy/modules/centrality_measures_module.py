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
            "weighted_degree": {
                "type": "boolean",
                "default": True,
                "doc": "Boolean to indicate whether to calculate weighted degree or not. Conditions for calculation can be set in 'weight_column_name'.",
            },
            "weight_column_name": {
                "type": "string",
                "default": "",
                "doc": "The name of the column in the edge table containing data for the 'weight' or 'strength' of an edge. If there is a column already named 'weight', this will be automatically selected. If otherwise left empty, weighted degree is calculated by aggregrating parallel edges where edge weight is assigned a weight of 1.",
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
        import pandas as pd

        from kiara_plugin.tropy.models import NetworkGraph

        edges = inputs.get_value_obj("network_graph")
        wd = inputs.get_value_data("weighted_degree")
        weight_name = inputs.get_value_data("weight_column_name")

        network_data: NetworkGraph = edges.data

        G = network_data.as_networkx_graph()
        G.remove_edges_from(list(nx.selfloop_edges(G)))

        degree = {}
        for node in G:
            degree[node] = G.degree(node)
        nx.set_node_attributes(G, degree, "Degree Score")

        attribute_network = NetworkGraph.create_from_networkx_graph(G)

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
            "weighted_betweenness": {
                "type": "boolean",
                "default": True,
                "doc": "Boolean to indicate whether to calculate weighted betweenness as well as unweighted betweenness.",
            },
            "weight_column_name": {
                "type": "string",
                "default": "",
                "doc": "The name of the column in the edge table containing data for the 'weight' of an edge. If there is a column already named 'weight', this will be automatically selected. If otherwise left empty, weight is calculated by aggregrating parallel edges where edge weight is assigned a weight of 1.",
            },
            "weight_meaning": {
                "type": "boolean",
                "default": True,
                "doc": "How the weights given should be interpreted. If 'True', weight will be defined positively as 'strength', and these edges will be prioritised in shortest path calculations. If 'False', weight will be defined negatively as 'cost' or 'distance', and these edges will be avoided in shortest path calculations.",
            },
        }

    def create_outputs_schema(self):
        return {
            "network_result": {
                "type": "table",
                "doc": "A table showing the rank and raw score for betweenness centrality.",
            },
            "centrality_network": {
                "type": "network_graph",
                "doc": "Updated network data with betweenness ranking assigned as a node attribute.",
            },
        }

    def process(self, inputs: ValueMap, outputs: ValueMap):

        import networkx as nx
        import pandas as pd

        from kiara_plugin.tropy.models import NetworkGraph

        edges = inputs.get_value_obj("network_graph")
        wd = inputs.get_value_data("weighted_betweenness")
        weight_name = inputs.get_value_data("weight_column_name")
        wm = inputs.get_value_data("weight_meaning")

        network_data: NetworkGraph = edges.data

        G = network_data.as_networkx_graph()
        G.remove_edges_from(list(nx.selfloop_edges(G)))

        between = nx.betweenness_centrality(G)
        nx.set_node_attributes(G, between, "Betweenness Score")

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
            "iterations": {"type": "integer", "default": 1000},
            "weighted_eigenvector": {
                "type": "boolean",
                "default": True,
                "doc": "Boolean to indicate whether to calculate weighted eigenvector as well as unweighted eigenvector.",
            },
            "weight_column_name": {
                "type": "string",
                "default": "",
                "doc": "The name of the column in the edge table containing data for the 'weight' of an edge. If there is a column already named 'weight', this will be automatically selected. If otherwise left empty, weight is calculated by aggregrating parallel edges where edge weight is assigned a weight of 1.",
            },
            "weight_meaning": {
                "type": "boolean",
                "default": True,
                "doc": "How the weights given should be interpreted. If 'True', weight will be defined positively as 'strength', and these edges will be prioritised in shortest path calculations. If 'False', weight will be defined negatively as 'cost' or 'distance', and these edges will be avoided in shortest path calculations.",
            },
        }

    def create_outputs_schema(self):
        return {
            "network_result": {
                "type": "table",
                "doc": "A table showing the rank and raw score for eigenvector centrality.",
            },
            "centrality_network": {
                "type": "network_graph",
                "doc": "Updated network data with eigenvector ranking assigned as a node attribute.",
            },
        }

    def process(self, inputs: ValueMap, outputs: ValueMap):

        import networkx as nx
        import pandas as pd

        from kiara_plugin.tropy.models import NetworkGraph

        edges = inputs.get_value_obj("network_graph")
        iterations = inputs.get_value_data("iterations")
        wd = inputs.get_value_data("weighted_eigenvector")
        weight_name = inputs.get_value_data("weight_column_name")
        wm = inputs.get_value_data("weight_meaning")

        network_data: NetworkGraph = edges.data

        G = network_data.as_networkx_graph()
        G.remove_edges_from(list(nx.selfloop_edges(G)))

        eigenvector = nx.eigenvector_centrality(G, max_iter=iterations)
        nx.set_node_attributes(G, eigenvector, "Eigenvector Score")

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
            "weighted_closeness": {
                "type": "boolean",
                "default": True,
                "doc": "Boolean to indicate whether to calculate weighted closeness as well as unweighted closeness.",
            },
            "weight_column_name": {
                "type": "string",
                "default": "",
                "doc": "The name of the column in the edge table containing data for the 'weight' of an edge. If there is a column already named 'weight', this will be automatically selected. If otherwise left empty, weight is calculated by aggregrating parallel edges where edge weight is assigned a weight of 1.",
            },
            "weight_meaning": {
                "type": "boolean",
                "default": True,
                "doc": "How the weights given should be interpreted. If 'True', weight will be defined positively as 'strength', and these edges will be prioritised in shortest path calculations. If 'False', weight will be defined negatively as 'cost' or 'distance', and these edges will be avoided in shortest path calculations.",
            },
        }

    def create_outputs_schema(self):
        return {
            "network_result": {
                "type": "table",
                "doc": "A table showing the rank and raw score for closeness centrality.",
            },
            "centrality_network": {
                "type": "network_graph",
                "doc": "Updated network data with closeness ranking assigned as a node attribute.",
            },
        }

    def process(self, inputs: ValueMap, outputs: ValueMap):

        import networkx as nx
        import pandas as pd

        from kiara_plugin.tropy.models import NetworkGraph

        edges = inputs.get_value_obj("network_graph")
        wd = inputs.get_value_data("weighted_closeness")
        weight_name = inputs.get_value_data("weight_column_name")
        wm = inputs.get_value_data("weight_meaning")

        network_data: NetworkGraph = edges.data

        G = network_data.as_networkx_graph()
        G.remove_edges_from(list(nx.selfloop_edges(G)))

        def result_func(list):
            rank, count, previous, result = (0, 0, None, {})
            for key, num in list:
                count += 1
                if num != previous:
                    rank += count
                    previous = num
                    count = 0
                result[key] = num, rank
            return result

        closeness = nx.closeness_centrality(G)
        nx.set_node_attributes(G, closeness, "Closeness Score")
        sorted_dict = [
            [item[1][1], item[0], item[1][0]]
            for item in sorted(
                result_func(
                    sorted(closeness.items(), key=itemgetter(1), reverse=True)
                ).items(),
                key=itemgetter(1),
                reverse=True,
            )
        ]

        df = pd.DataFrame(sorted_dict)
        df.columns = pd.Index(["Rank", "Node", "Score"])

        if wd is True:
            graph = network_data.as_networkx_graph()
            edge_weight = nx.get_edge_attributes(graph, weight_name)
            for u, v, key in edge_weight:
                nx.set_edge_attributes(graph, edge_weight, "weight")

            if wm is True:
                for u, v, d in graph.edges(data=True):
                    d["weight"] == (1 / d["weight"])

            weight_closeness = nx.closeness_centrality(graph, weight="weight")
            nx.set_node_attributes(G, weight_closeness, "Weighted Closeness Score")

            df2 = pd.DataFrame(
                list(weight_closeness.items()), columns=["Node", "Weighted Closeness"]
            )
            df = df.merge(df2, how="left", on="Node").reset_index(drop=True)

        attribute_network = NetworkGraph.create_from_networkx_graph(G)

        outputs.set_values(network_result=df, centrality_network=attribute_network)
