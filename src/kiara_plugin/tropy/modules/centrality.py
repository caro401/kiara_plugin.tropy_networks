# -*- coding: utf-8 -*-
from typing import Any, ClassVar, Dict, Mapping

from pydantic import Field

from kiara.api import KiaraModule
from kiara.models.module import KiaraModuleConfig
from kiara_plugin.network_analysis.defaults import ATTRIBUTE_PROPERTY_KEY
from kiara_plugin.network_analysis.models import NetworkData
from kiara_plugin.tropy.defaults import (
    DEFAULT_UNWEIGHTED_BETWEENNESS_COLUMN_NAME,
    DEFAULT_UNWEIGHTED_NODE_DEGREE_COLUMN_NAME,
    DEFAULT_WEIGHTED_BETWEENNESS_COLUMN_NAME,
    DEFAULT_WEIGHTED_NODE_DEGREE_COLUMN_NAME,
    UNWEIGHTED_NODE_BETWEENNESS_TEXT,
    UNWEIGHTED_NODE_DEGREE_TEXT,
    WEIGHTED_NODE_DEGREE_TEXT,
)

KIARA_METADATA = {
    "description": "Kiara modules related to centrality calculations",
}


class CentralityModuleConfig(KiaraModuleConfig):
    """Configuration for degree ranking module."""

    weighted: bool = Field(
        description="Whether to calculate the weighted degree or not.", default=False
    )
    target_column_name: str = Field(
        description="The name of the column in the nodes table containing the node degree.",
        default=DEFAULT_UNWEIGHTED_NODE_DEGREE_COLUMN_NAME,
    )


class DegreeRankingModule(KiaraModule):
    """Calculate degree centrality ranking for nodes in a network.

    Unweighted degree centrality uses an undirected graph and measures the number of independent connections each node has.
    Weighted degree centrality uses a directed graph and measures the total number of connections or weight attached to a node.

    Uses networkx degree.
    https://networkx.org/documentation/stable/reference/generated/networkx.classes.function.degree.html"""

    _module_type_name: ClassVar[str] = "tropy.network_data.calculate_nodes_degree"
    _config_cls = CentralityModuleConfig

    @classmethod
    def retrieve_included_operations(cls) -> Mapping[str, Mapping[str, Any]]:
        result = {
            "tropy.network_data.calculate_nodes_degree_weighted": {
                "module_config": {
                    "weighted": True,
                },
                "doc": "Calculate weighted degree centrality ranking for nodes in a network.",
            }
        }
        return result

    KIARA_METADATA: ClassVar[Dict[str, Any]] = {
        "references": {
            "discussion": {
                "url": "https://github.com/DHARPA-Project/kiara_plugin.network_analysis/discussions/26"
            }
        }
    }

    def create_inputs_schema(self):
        result = {
            "network_data": {
                "type": "network_data",
                "doc": "The network graph being queried.",
            }
        }
        if self.get_config_value("weighted"):

            result["weight_column_name"] = {
                "type": "string",
                "default": "",
                "doc": "The name of the column in the edge table containing data for the 'weight' or 'strength' of an edge. If there is a column already named 'weight', this will be automatically selected. If otherwise left empty, weighted degree is calculated by aggregrating parallel edges where edge weight is assigned a weight of 1.",
            }

        return result

    def create_outputs_schema(self):
        if self.get_config_value("weighted"):
            txt = " weighted"
        else:
            txt = " "
        return {
            "network_data": {
                "type": "network_data",
                "doc": f"The network data, augmented including a a per-node{txt} centrality measure.",
            },
        }

    def process(self, inputs, outputs):

        import numpy as np
        import pyarrow as pa

        from kiara_plugin.network_analysis.models.metadata import (
            NetworkEdgeAttributeMetadata,
        )

        edges = inputs.get_value_obj("network_data")

        use_weighted = self.get_config_value("weighted")
        if use_weighted:
            weight_name = inputs.get_value_data("weight_column_name")

        network_data: NetworkData = edges.data

        num_nodes = network_data.num_nodes
        random_centrality_data = pa.array(np.random.rand(num_nodes))

        NODE_CENTRALITY_COLUMN_METADATA = NetworkEdgeAttributeMetadata(
            doc=UNWEIGHTED_NODE_DEGREE_TEXT, computed_attribute=True
        )  # type: ignore

        NODE_WEIGHTED_CENTRALITY_COLUMN_METADATA = NetworkEdgeAttributeMetadata(
            doc=WEIGHTED_NODE_DEGREE_TEXT, computed_attribute=True
        )  # type: ignore

        if use_weighted:
            weight_name = inputs.get_value_data("weight_column_name")
            centrality_column_name = (
                f"{DEFAULT_WEIGHTED_NODE_DEGREE_COLUMN_NAME}{weight_name}"
            )
            metadata = NODE_WEIGHTED_CENTRALITY_COLUMN_METADATA
        else:
            centrality_column_name = DEFAULT_UNWEIGHTED_NODE_DEGREE_COLUMN_NAME
            metadata = NODE_CENTRALITY_COLUMN_METADATA

        augmented_node_columns = {centrality_column_name: random_centrality_data}

        augmented_node_columns_md = {
            centrality_column_name: {
                ATTRIBUTE_PROPERTY_KEY: metadata,
            }
        }

        cloned = NetworkData.create_augmented(
            network_data,
            additional_nodes_columns=augmented_node_columns,
            nodes_column_metadata=augmented_node_columns_md,
        )

        outputs.set_values(network_data=cloned)

        #
        #
        # G = network_data.as_networkx_graph(nx.Graph)
        # G.remove_edges_from(list(nx.selfloop_edges(G)))
        #
        # def result_func(list):
        #     rank, count, previous, result = (0, 0, None, {})
        #     for key, num in list:
        #         count += 1
        #         if num != previous:
        #             rank += count
        #             previous = num
        #             count = 0
        #         result[key] = num, rank
        #     return result
        #
        # degree = {}
        # for node in G:
        #     degree[node] = G.degree(node)
        # nx.set_node_attributes(G, degree, "Degree Score")
        #
        # sorted_dict = [
        #     [item[1][1], item[0], item[1][0]]
        #     for item in sorted(
        #         result_func(
        #             sorted(degree.items(), key=itemgetter(1), reverse=True)
        #         ).items(),
        #         key=itemgetter(1),
        #         reverse=True,
        #     )
        # ]
        #
        # df = pd.DataFrame(sorted_dict, columns=["Rank", "Node", "Degree"])
        #
        # if wd == True:
        #     if weight_name == "":
        #         MG = network_data.as_networkx_graph(nx.MultiDiGraph)
        #
        #         graph = nx.DiGraph()
        #         for u, v, data in MG.edges(data=True):
        #             w = data["weight"] if "weight" in data else 1
        #             if graph.has_edge(u, v):
        #                 graph[u][v]["weight"] += w
        #             else:
        #                 graph.add_edge(u, v, weight=w)
        #
        #         weight_degree = {}
        #         for node in graph:
        #             weight_degree[node] = graph.degree(node, weight="weight")
        #         nx.set_node_attributes(G, weight_degree, "Weighted Degree Score")
        #
        #         edge_weight = nx.get_edge_attributes(graph, "weight")
        #         nx.set_edge_attributes(G, edge_weight, "weight")
        #
        #         df2 = pd.DataFrame(
        #             list(weight_degree.items()), columns=["Node", "Weighted Degree"]
        #         )
        #         df = df.merge(df2, how="left", on="Node").reset_index(drop=True)
        #
        #     if weight_name != "":
        #         MG = network_data.as_networkx_graph(nx.MultiDiGraph)
        #         edge_weight = nx.get_edge_attributes(MG, weight_name)
        #         for u, v, key in edge_weight:
        #             nx.set_edge_attributes(MG, edge_weight, "weight")
        #
        #         graph = nx.DiGraph()
        #         for u, v, data in MG.edges(data=True):
        #             w = data["weight"] if "weight" in data else 1
        #             if graph.has_edge(u, v):
        #                 graph[u][v]["weight"] += w
        #             else:
        #                 graph.add_edge(u, v, weight=w)
        #
        #         weight_degree = {}
        #         for node in graph:
        #             weight_degree[node] = graph.degree(node, weight="weight")
        #         nx.set_node_attributes(G, weight_degree, "Weighted Degree Score")
        #
        #         df2 = pd.DataFrame(
        #             list(weight_degree.items()), columns=["Node", "Weighted Degree"]
        #         )
        #         df = df.merge(df2, how="left", on="Node").reset_index(drop=True)
        #
        # attribute_network = NetworkData.create_from_networkx_graph(G)
        #
        # outputs.set_values(network_result=df, centrality_network=attribute_network)


class Betweenness_Ranking(KiaraModule):
    """Creates an ordered table with the rank and raw score for betweenness centrality.
    Betweenness centrality measures the percentage of all shortest paths that a node appears on, therefore measuring the likeliness that a node may act as a connector or 'intermediary'.

    Uses a directed graph and networkx.betweenness_centrality()
    https://networkx.org/documentation/stable/reference/algorithms/generated/networkx.algorithms.centrality.betweenness_centrality.html#networkx.algorithms.centrality.betweenness_centrality"""

    _module_type_name: ClassVar[str] = "tropy.network_data.calculate_betweenness"
    _config_cls = CentralityModuleConfig

    @classmethod
    def retrieve_included_operations(cls) -> Mapping[str, Mapping[str, Any]]:
        result = {
            "tropy.network_data.calculate_betweenness_weighted": {
                "module_config": {
                    "weighted": True,
                },
                "doc": "Calculate weighted betweenness for nodes in a network.",
            }
        }
        return result

    def create_inputs_schema(self):
        result = {
            "network_data": {
                "type": "network_data",
                "doc": "The network graph being queried.",
            }
        }

        if self.get_config_value("weighted"):
            result["weight_column_name"] = {
                "type": "string",
                "doc": "The name of the column in the edge table containing data for the 'weight' of an edge. If there is a column already named 'weight', this will be automatically selected. If otherwise left empty, weight is calculated by aggregrating parallel edges where edge weight is assigned a weight of 1.",
            }
            result["weight_meaning"] = {
                "type": "boolean",
                "default": True,
                "doc": "How the weights given should be interpreted. If 'True', weight will be defined positively as 'strength', and these edges will be prioritised in shortest path calculations. If 'False', weight will be defined negatively as 'cost' or 'distance', and these edges will be avoided in shortest path calculations.",
            }

        return result

    def create_outputs_schema(self):
        if self.get_config_value("weighted"):
            txt = " weighted"
        else:
            txt = " "
        return {
            "network_data": {
                "type": "network_data",
                "doc": f"Updated network data with{txt} betweenness ranking assigned as a node attribute.",
            },
        }

    def process(self, inputs, outputs):
        import numpy as np
        import pyarrow as pa

        from kiara_plugin.network_analysis.models.metadata import (
            NetworkEdgeAttributeMetadata,
        )

        edges = inputs.get_value_obj("network_data")

        use_weighted = self.get_config_value("weighted")
        if use_weighted:
            weight_name = inputs.get_value_data("weight_column_name")
            weight_meaning = inputs.get_value_data("weight_meaning")

        network_data: NetworkData = edges.data

        num_nodes = network_data.num_nodes
        random_centrality_data = pa.array(np.random.rand(num_nodes))

        NODE_BETWEENNESS_COLUMN_METADATA = NetworkEdgeAttributeMetadata(doc=UNWEIGHTED_NODE_BETWEENNESS_TEXT, computed_attribute=True)  # type: ignore

        NODE_WEIGHTED_BETWEENNESS_COLUMN_METADATA = NetworkEdgeAttributeMetadata(
            doc=UNWEIGHTED_NODE_BETWEENNESS_TEXT, computed_attribute=True
        )  # type: ignore

        if use_weighted:
            weight_name = inputs.get_value_data("weight_column_name")
            weight_meaning = inputs.get_value_data("weight_meaning")
            if weight_meaning:
                txt = "strength"
            else:
                txt = "shortest_path"
            betweenness_column_name = (
                f"{DEFAULT_WEIGHTED_BETWEENNESS_COLUMN_NAME}{weight_name}_{txt}"
            )
            metadata = NODE_WEIGHTED_BETWEENNESS_COLUMN_METADATA
        else:
            betweenness_column_name = DEFAULT_UNWEIGHTED_BETWEENNESS_COLUMN_NAME
            metadata = NODE_BETWEENNESS_COLUMN_METADATA

        augmented_node_columns = {betweenness_column_name: random_centrality_data}

        augmented_node_columns_md = {
            betweenness_column_name: {
                ATTRIBUTE_PROPERTY_KEY: metadata,
            }
        }

        cloned = NetworkData.create_augmented(
            network_data,
            additional_nodes_columns=augmented_node_columns,
            nodes_column_metadata=augmented_node_columns_md,
        )

        outputs.set_values(network_data=cloned)


#        G = network_data.as_networkx_graph(nx.DiGraph)
#        G.remove_edges_from(list(nx.selfloop_edges(G)))
#
#         def result_func(list):
#             rank, count, previous, result = (0, 0, None, {})
#             for key, num in list:
#                 count += 1
#                 if num != previous:
#                     rank += count
#                     previous = num
#                     count = 0
#                 result[key] = num, rank
#             return result
#
#         between = nx.betweenness_centrality(G)
#         nx.set_node_attributes(G, between, "Betweenness Score")
#         sorted_dict = [
#             [item[1][1], item[0], item[1][0]]
#             for item in sorted(
#                 result_func(
#                     sorted(between.items(), key=itemgetter(1), reverse=True)
#                 ).items(),
#                 key=itemgetter(1),
#                 reverse=True,
#             )
#         ]
#
#         df = pd.DataFrame(sorted_dict)
#
#         df.columns = ["Rank", "Node", "Score"]
#
#         if wd == True:
#             graph = network_data.as_networkx_graph(nx.DiGraph)
#             edge_weight = nx.get_edge_attributes(graph, weight_name)
#             for u, v, key in edge_weight:
#                 nx.set_edge_attributes(graph, edge_weight, "weight")
#
#             if wm == True:
#                 for u, v, d in graph.edges(data=True):
#                     d["weight"] == (1 / d["weight"])
#
#             weight_betweenness = nx.betweenness_centrality(graph, weight="weight")
#             nx.set_node_attributes(G, weight_betweenness, "Weighted Betweenness Score")
#
#             df2 = pd.DataFrame(
#                 list(weight_betweenness.items()),
#                 columns=["Node", "Weighted Betweenness"],
#             )
#             df = df.merge(df2, how="left", on="Node").reset_index(drop=True)
#
#         attribute_network = NetworkData.create_from_networkx_graph(G)
#
#         outputs.set_values(network_result=df, centrality_network=attribute_network)
#
#
# class Eigenvector_Ranking(KiaraModule):
#     """Creates an ordered table with the rank and raw score for betweenness centrality.
#     Eigenvector centrality measures the extent to which a node is connected to other nodes of importance or influence.
#
#     Uses an undirected graph networkx.eigenvector_centrality()
#     https://networkx.org/documentation/stable/reference/algorithms/generated/networkx.algorithms.centrality.eigenvector_centrality.html#networkx.algorithms.centrality.eigenvector_centrality"""
#
#     _module_type_name = "tropy.create.eigenvector_rank_list"
#
#     def create_inputs_schema(self):
#         return {
#             "network_data": {
#                 "type": "network_data",
#                 "doc": "The network graph being queried.",
#             },
#             "iterations": {"type": "integer", "default": 1000},
#             "weighted_eigenvector": {
#                 "type": "boolean",
#                 "default": True,
#                 "doc": "Boolean to indicate whether to calculate weighted eigenvector as well as unweighted eigenvector.",
#             },
#             "weight_column_name": {
#                 "type": "string",
#                 "default": "",
#                 "doc": "The name of the column in the edge table containing data for the 'weight' of an edge. If there is a column already named 'weight', this will be automatically selected. If otherwise left empty, weight is calculated by aggregrating parallel edges where edge weight is assigned a weight of 1.",
#             },
#             "weight_meaning": {
#                 "type": "boolean",
#                 "default": True,
#                 "doc": "How the weights given should be interpreted. If 'True', weight will be defined positively as 'strength', and these edges will be prioritised in shortest path calculations. If 'False', weight will be defined negatively as 'cost' or 'distance', and these edges will be avoided in shortest path calculations.",
#             },
#         }
#
#     def create_outputs_schema(self):
#         return {
#             "network_result": {
#                 "type": "table",
#                 "doc": "A table showing the rank and raw score for eigenvector centrality.",
#             },
#             "centrality_network": {
#                 "type": "network_data",
#                 "doc": "Updated network data with eigenvector ranking assigned as a node attribute.",
#             },
#         }
#
#     def process(self, inputs, outputs):
#         edges = inputs.get_value_obj("network_data")
#         iterations = inputs.get_value_data("iterations")
#         wd = inputs.get_value_data("weighted_eigenvector")
#         weight_name = inputs.get_value_data("weight_column_name")
#         wm = inputs.get_value_data("weight_meaning")
#
#         network_data: NetworkData = (
#             edges.data
#         )  # check the source for the NetworkData class to see what
#         # convenience methods it can give you:
#         # https://github.com/DHARPA-Project/kiara_plugin.network_analysis/blob/develop/src/kiara_plugin/network_analysis/models.py#L52
#
#         G = network_data.as_networkx_graph(nx.Graph)
#         G.remove_edges_from(list(nx.selfloop_edges(G)))
#
#         def result_func(list):
#             rank, count, previous, result = (0, 0, None, {})
#             for key, num in list:
#                 count += 1
#                 if num != previous:
#                     rank += count
#                     previous = num
#                     count = 0
#                 result[key] = num, rank
#             return result
#
#         eigenvector = nx.eigenvector_centrality(G, max_iter=iterations)
#         nx.set_node_attributes(G, eigenvector, "Eigenvector Score")
#         sorted_dict = [
#             [item[1][1], item[0], item[1][0]]
#             for item in sorted(
#                 result_func(
#                     sorted(eigenvector.items(), key=itemgetter(1), reverse=True)
#                 ).items(),
#                 key=itemgetter(1),
#                 reverse=True,
#             )
#         ]
#
#         df = pd.DataFrame(sorted_dict)
#         df.columns = ["Rank", "Node", "Score"]
#
#         if wd == True:
#             graph = network_data.as_networkx_graph(nx.Graph)
#             edge_weight = nx.get_edge_attributes(graph, weight_name)
#             for u, v, key in edge_weight:
#                 nx.set_edge_attributes(graph, edge_weight, "weight")
#
#             if wm == False:
#                 for u, v, d in graph.edges(data=True):
#                     d["weight"] == (1 / d["weight"])
#
#             weight_eigenvector = nx.eigenvector_centrality(
#                 graph, weight="weight", max_iter=100000
#             )
#             nx.set_node_attributes(G, weight_eigenvector, "Weighted Eigenvector Score")
#
#             df2 = pd.DataFrame(
#                 list(weight_eigenvector.items()),
#                 columns=["Node", "Weighted Eigenvector"],
#             )
#             df = df.merge(df2, how="left", on="Node").reset_index(drop=True)
#
#         attribute_network = NetworkData.create_from_networkx_graph(G)
#
#         outputs.set_values(network_result=df, centrality_network=attribute_network)
#
#
# class Closeness_Ranking(KiaraModule):
#     """Creates an ordered table with the rank and raw score for closeness centrality.
#     Closeness centrality measures the average shortest distance path between a node and all reachable nodes in the network.
#
#     Uses a directed graph and networkx.closeness_centrality()
#     https://networkx.org/documentation/stable/reference/algorithms/generated/networkx.algorithms.centrality.closeness_centrality.html#networkx.algorithms.centrality.closeness_centrality"""
#
#     _module_type_name = "tropy.create.closeness_rank_list"
#
#     def create_inputs_schema(self):
#         return {
#             "network_data": {
#                 "type": "network_data",
#                 "doc": "The network graph being queried.",
#             },
#             "weighted_closeness": {
#                 "type": "boolean",
#                 "default": True,
#                 "doc": "Boolean to indicate whether to calculate weighted closeness as well as unweighted closeness.",
#             },
#             "weight_column_name": {
#                 "type": "string",
#                 "default": "",
#                 "doc": "The name of the column in the edge table containing data for the 'weight' of an edge. If there is a column already named 'weight', this will be automatically selected. If otherwise left empty, weight is calculated by aggregrating parallel edges where edge weight is assigned a weight of 1.",
#             },
#             "weight_meaning": {
#                 "type": "boolean",
#                 "default": True,
#                 "doc": "How the weights given should be interpreted. If 'True', weight will be defined positively as 'strength', and these edges will be prioritised in shortest path calculations. If 'False', weight will be defined negatively as 'cost' or 'distance', and these edges will be avoided in shortest path calculations.",
#             },
#         }
#
#     def create_outputs_schema(self):
#         return {
#             "network_result": {
#                 "type": "table",
#                 "doc": "A table showing the rank and raw score for closeness centrality.",
#             },
#             "centrality_network": {
#                 "type": "network_data",
#                 "doc": "Updated network data with closeness ranking assigned as a node attribute.",
#             },
#         }
#
#     def process(self, inputs, outputs):
#         edges = inputs.get_value_obj("network_data")
#         wd = inputs.get_value_data("weighted_closeness")
#         weight_name = inputs.get_value_data("weight_column_name")
#         wm = inputs.get_value_data("weight_meaning")
#
#         network_data: NetworkData = (
#             edges.data
#         )  # check the source for the NetworkData class to see what
#         # convenience methods it can give you:
#         # https://github.com/DHARPA-Project/kiara_plugin.network_analysis/blob/develop/src/kiara_plugin/network_analysis/models.py#L52
#
#         G = network_data.as_networkx_graph(nx.DiGraph)
#         G.remove_edges_from(list(nx.selfloop_edges(G)))
#
#         def result_func(list):
#             rank, count, previous, result = (0, 0, None, {})
#             for key, num in list:
#                 count += 1
#                 if num != previous:
#                     rank += count
#                     previous = num
#                     count = 0
#                 result[key] = num, rank
#             return result
#
#         closeness = nx.closeness_centrality(G)
#         nx.set_node_attributes(G, closeness, "Closeness Score")
#         sorted_dict = [
#             [item[1][1], item[0], item[1][0]]
#             for item in sorted(
#                 result_func(
#                     sorted(closeness.items(), key=itemgetter(1), reverse=True)
#                 ).items(),
#                 key=itemgetter(1),
#                 reverse=True,
#             )
#         ]
#
#         df = pd.DataFrame(sorted_dict)
#         df.columns = ["Rank", "Node", "Score"]
#
#         if wd == True:
#             graph = network_data.as_networkx_graph(nx.DiGraph)
#             edge_weight = nx.get_edge_attributes(graph, weight_name)
#             for u, v, key in edge_weight:
#                 nx.set_edge_attributes(graph, edge_weight, "weight")
#
#             if wm == True:
#                 for u, v, d in graph.edges(data=True):
#                     d["weight"] == (1 / d["weight"])
#
#             weight_closeness = nx.closeness_centrality(graph, weight="weight")
#             nx.set_node_attributes(G, weight_closeness, "Weighted Closeness Score")
#
#             df2 = pd.DataFrame(
#                 list(weight_closeness.items()), columns=["Node", "Weighted Closeness"]
#             )
#             df = df.merge(df2, how="left", on="Node").reset_index(drop=True)
#
#         attribute_network = NetworkData.create_from_networkx_graph(G)
#
#         outputs.set_values(network_result=df, centrality_network=attribute_network)
