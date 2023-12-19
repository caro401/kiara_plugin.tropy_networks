# -*- coding: utf-8 -*-
# from typing import Any, ClassVar, Dict
#
# from pydantic import Field
#
# from kiara.api import KiaraModule
# from kiara.models.module import KiaraModuleConfig
# from kiara_plugin.network_analysis.defaults import (
#     ATTRIBUTE_PROPERTY_KEY,
#     UNWEIGHTED_NODE_DEGREE_TEXT,
# )
# from kiara_plugin.network_analysis.models import NetworkData
# from kiara_plugin.tropy.defaults import (
#     DEFAULT_WEIGHTED_NODE_DEGREE_COLUMN_NAME,
#     WEIGHTED_NODE_DEGREE_TEXT,
# )
#
# KIARA_METADATA = {
#     "description": "Kiara modules related to centrality calculations",
# }
#
#
# class NodesDegreeModuleConfig(KiaraModuleConfig):
#     """Configuration for degree ranking module."""
#
#     target_column_name: str = Field(
#         description="The name of the column in the nodes table containing the node degree.",
#         default=DEFAULT_WEIGHTED_NODE_DEGREE_COLUMN_NAME,
#     )
#
#
# class WeightedNodesDegreeModule(KiaraModule):
#     """Calculate degree centrality ranking for nodes in a network.
#
#     Unweighted degree centrality uses an undirected graph and measures the number of independent connections each node has.
#     Weighted degree centrality uses a directed graph and measures the total number of connections or weight attached to a node.
#
#     Uses networkx degree.
#     https://networkx.org/documentation/stable/reference/generated/networkx.classes.function.degree.html"""
#
#     _module_type_name: ClassVar[str] = "tropy.network_data.calculate_nodes_degree"
#     _config_cls = NodesDegreeModuleConfig
#
#     KIARA_METADATA: ClassVar[Dict[str, Any]] = {
#         "references": {
#             "discussion": {
#                 "url": "https://github.com/DHARPA-Project/kiara_plugin.network_analysis/discussions/26"
#             }
#         }
#     }
#
#     def create_inputs_schema(self):
#         result = {
#             "network_data": {
#                 "type": "network_data",
#                 "doc": "The network graph being queried.",
#             }
#         }
#         result["weight_column_name"] = {
#             "type": "string",
#             "default": "",
#             "doc": "The name of the column in the edge table containing data for the 'weight' or 'strength' of an edge. If there is a column already named 'weight', this will be automatically selected. If otherwise left empty, weighted degree is calculated by aggregrating parallel edges where edge weight is assigned a weight of 1.",
#         }
#
#         return result
#
#     def create_outputs_schema(self):
#         txt = " weighted"
#
#         return {
#             "network_data": {
#                 "type": "network_data",
#                 "doc": f"The network data, augmented including a a per-node{txt} centrality measure.",
#             },
#         }
#
#     def process(self, inputs, outputs):
#
#         import numpy as np
#         import pyarrow as pa
#
#         from kiara_plugin.network_analysis.models.metadata import (
#             NetworkEdgeAttributeMetadata,
#         )
#
#         edges = inputs.get_value_obj("network_data")
#
#         weight_name = inputs.get_value_data("weight_column_name")
#
#         network_data: NetworkData = edges.data
#
#         num_nodes = network_data.num_nodes
#         random_centrality_data = pa.array(np.random.rand(num_nodes))
#
#         NODE_CENTRALITY_COLUMN_METADATA = NetworkEdgeAttributeMetadata(
#             doc=UNWEIGHTED_NODE_DEGREE_TEXT, computed_attribute=True
#         )  # type: ignore
#
#         NODE_WEIGHTED_CENTRALITY_COLUMN_METADATA = NetworkEdgeAttributeMetadata(
#             doc=WEIGHTED_NODE_DEGREE_TEXT, computed_attribute=True
#         )  # type: ignore
#
#         weight_name = inputs.get_value_data("weight_column_name")
#         centrality_column_name = (
#             f"{DEFAULT_WEIGHTED_NODE_DEGREE_COLUMN_NAME}{weight_name}"
#         )
#         metadata = NODE_WEIGHTED_CENTRALITY_COLUMN_METADATA
#
#         augmented_node_columns = {centrality_column_name: random_centrality_data}
#
#         augmented_node_columns_md = {
#             centrality_column_name: {
#                 ATTRIBUTE_PROPERTY_KEY: metadata,
#             }
#         }
#
#         cloned = NetworkData.create_augmented(
#             network_data,
#             additional_nodes_columns=augmented_node_columns,
#             nodes_column_metadata=augmented_node_columns_md,
#         )
#
#         outputs.set_values(network_data=cloned)

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
