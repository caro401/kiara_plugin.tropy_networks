# -*- coding: utf-8 -*-
from operator import itemgetter
from typing import Any, Mapping

import networkx as nx
import pandas as pd
from pydantic import Field

from kiara.api import KiaraModule
from kiara.models.module import KiaraModuleConfig
from kiara_plugin.network_analysis.models import NetworkData
from kiara_plugin.tropy.defaults import DEFAULT_UNWEIGHTED_NODE_DEGREE_COLUMN_NAME

KIARA_METADATA = {
    "authors": [
        {"name": "Markus Binsteiner", "email": "markus@frkl.io"},
        {"name": "Caitlin Burge", "email": "caitlin.burge@uni.lu"},
    ],
    "description": "Kiara modules related to centrality calculations",
}


class DegreeRankingModuleConfig(KiaraModuleConfig):
    """Configuration for degree ranking module."""

    use_weighted_degree: bool = Field(
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

    _module_type_name = "tropy.network_data.calculate_nodes_degree"
    _config_cls = DegreeRankingModuleConfig

    @classmethod
    def retrieve_included_operations(cls) -> Mapping[str, Mapping[str, Any]]:
        result = {
            "tropy.network_data.calculate_nodes_degree_weighted": {
                "module_config": {
                    "use_weighted_degree": True,
                },
                "doc": "xxx",
            }
        }
        return result

    KIARA_METADATA = {
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
        if self.get_config_value("use_weighted_degree"):

            result["weight_column_name"] = {
                "type": "string",
                "default": "",
                "doc": "The name of the column in the edge table containing data for the 'weight' or 'strength' of an edge. If there is a column already named 'weight', this will be automatically selected. If otherwise left empty, weighted degree is calculated by aggregrating parallel edges where edge weight is assigned a weight of 1.",
            }

        return result

    def create_outputs_schema(self):
        return {
            "network_result": {
                "type": "table",
                "doc": "A table showing the rank and raw score for degree centrality.",
            },
            "centrality_network": {
                "type": "network_data",
                "doc": "Updated network data with degree ranking assigned as a node attribute.",
            },
        }

    def process(self, inputs, outputs):
        edges = inputs.get_value_obj("network_data")
        wd = inputs.get_value_data("weighted_degree")
        weight_name = inputs.get_value_data("weight_column_name")

        network_data: NetworkData = (
            edges.data
        )  # check the source for the NetworkData class to see what
        # convenience methods it can give you:
        # https://github.com/DHARPA-Project/kiara_plugin.network_analysis/blob/develop/src/kiara_plugin/network_analysis/models.py#L52

        G = network_data.as_networkx_graph(nx.Graph)
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

        degree = {}
        for node in G:
            degree[node] = G.degree(node)
        nx.set_node_attributes(G, degree, "Degree Score")

        sorted_dict = [
            [item[1][1], item[0], item[1][0]]
            for item in sorted(
                result_func(
                    sorted(degree.items(), key=itemgetter(1), reverse=True)
                ).items(),
                key=itemgetter(1),
                reverse=True,
            )
        ]

        df = pd.DataFrame(sorted_dict, columns=["Rank", "Node", "Degree"])

        if wd == True:
            if weight_name == "":
                MG = network_data.as_networkx_graph(nx.MultiDiGraph)

                graph = nx.DiGraph()
                for u, v, data in MG.edges(data=True):
                    w = data["weight"] if "weight" in data else 1
                    if graph.has_edge(u, v):
                        graph[u][v]["weight"] += w
                    else:
                        graph.add_edge(u, v, weight=w)

                weight_degree = {}
                for node in graph:
                    weight_degree[node] = graph.degree(node, weight="weight")
                nx.set_node_attributes(G, weight_degree, "Weighted Degree Score")

                edge_weight = nx.get_edge_attributes(graph, "weight")
                nx.set_edge_attributes(G, edge_weight, "weight")

                df2 = pd.DataFrame(
                    list(weight_degree.items()), columns=["Node", "Weighted Degree"]
                )
                df = df.merge(df2, how="left", on="Node").reset_index(drop=True)

            if weight_name != "":
                MG = network_data.as_networkx_graph(nx.MultiDiGraph)
                edge_weight = nx.get_edge_attributes(MG, weight_name)
                for u, v, key in edge_weight:
                    nx.set_edge_attributes(MG, edge_weight, "weight")

                graph = nx.DiGraph()
                for u, v, data in MG.edges(data=True):
                    w = data["weight"] if "weight" in data else 1
                    if graph.has_edge(u, v):
                        graph[u][v]["weight"] += w
                    else:
                        graph.add_edge(u, v, weight=w)

                weight_degree = {}
                for node in graph:
                    weight_degree[node] = graph.degree(node, weight="weight")
                nx.set_node_attributes(G, weight_degree, "Weighted Degree Score")

                df2 = pd.DataFrame(
                    list(weight_degree.items()), columns=["Node", "Weighted Degree"]
                )
                df = df.merge(df2, how="left", on="Node").reset_index(drop=True)

        attribute_network = NetworkData.create_from_networkx_graph(G)

        outputs.set_values(network_result=df, centrality_network=attribute_network)
