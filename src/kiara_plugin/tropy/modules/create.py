# -*- coding: utf-8 -*-
from typing import TYPE_CHECKING, Any, Mapping, Union

from pydantic import Field

from kiara.exceptions import KiaraProcessingException
from kiara.models.values.value import Value, ValueMap
from kiara.modules import KiaraModule, ValueMapSchema
from kiara.modules.included_core_modules.create_from import (
    CreateFromModule,
    CreateFromModuleConfig,
)
from kiara_plugin.tropy.defaults import (
    ALLOWED_GRAPH_TYPE_STRINGS,
    DEFAULT_NODE_ID_COLUMN_NAME,
    DEFAULT_SOURCE_COLUMN_NAME,
    DEFAULT_TARGET_COLUMN_NAME,
    GraphType,
)
from kiara_plugin.tropy.models import NetworkGraph

if TYPE_CHECKING:
    from kiara.models.filesystem import (
        KiaraFile,
    )
    from kiara_plugin.tabular.models import KiaraTable


KIARA_METADATA = {
    "authors": [
        {"name": "Lena Jaskov", "email": "helena.jaskov@uni.lu"},
        {"name": "Markus Binsteiner", "email": "markus@frkl.io"},
    ],
    "description": "Modules to create/export network data.",
}


class CreateNetworkDataModuleConfig(CreateFromModuleConfig):
    ignore_errors: bool = Field(
        description="Whether to ignore convert errors and omit the failed items.",
        default=False,
    )


class CreateNetworkDataModule(CreateFromModule):

    _module_type_name = "tropy.create.network_graph"
    _config_cls = CreateNetworkDataModuleConfig

    def create__network_graph__from__file(self, source_value: Value) -> Any:
        """Create a table from a file, trying to auto-determine the format of said file.

        Supported file formats (at the moment):
        - gml
        - gexf
        - graphml (uses the standard xml library present in Python, which is insecure - see xml for additional information. Only parse GraphML files you trust)
        - pajek
        - leda
        - graph6
        - sparse6
        """

        source_file: "KiaraFile" = source_value.data
        # the name of the attribute kiara should use to populate the node labels
        # label_attr_name: Union[str, None] = None
        # attributes to ignore when creating the node table,
        # mostly useful if we know that the file contains attributes that are not relevant for the network
        # or for 'label', if we don't want to duplicate the information in '_label' and 'label'

        if source_file.file_name.endswith(".gml"):
            import networkx as nx

            # we use 'lable="id"' here because networkx is fussy about labels being unique and non-null
            # we use the 'label' attribute for the node labels manually later
            graph = nx.read_gml(source_file.path, label="id")
            # label_attr_name = "label"

        elif source_file.file_name.endswith(".gexf"):
            import networkx as nx

            graph = nx.read_gexf(source_file.path)
        elif source_file.file_name.endswith(".graphml"):
            import networkx as nx

            graph = nx.read_graphml(source_file.path)
        elif source_file.file_name.endswith(".pajek") or source_file.file_name.endswith(
            ".net"
        ):
            import networkx as nx

            graph = nx.read_pajek(source_file.path)
        elif source_file.file_name.endswith(".leda"):
            import networkx as nx

            graph = nx.read_leda(source_file.path)
        elif source_file.file_name.endswith(
            ".graph6"
        ) or source_file.file_name.endswith(".g6"):
            import networkx as nx

            graph = nx.read_graph6(source_file.path)
        elif source_file.file_name.endswith(
            ".sparse6"
        ) or source_file.file_name.endswith(".s6"):
            import networkx as nx

            graph = nx.read_sparse6(source_file.path)
        else:
            supported_file_estensions = [
                "gml",
                "gexf",
                "graphml",
                "pajek",
                "leda",
                "graph6",
                "g6",
                "sparse6",
                "s6",
            ]

            msg = f"Can't create network data for unsupported format of file: {source_file.file_name}. Supported file extensions: {', '.join(supported_file_estensions)}"

            raise KiaraProcessingException(msg)

        return NetworkGraph.create_from_networkx_graph(
            graph=graph,
        )


class AssembleGraphFromTablesModule(KiaraModule):
    """Create a 'network_graph' instance from one or two tables.

    This module needs at least one table as input, providing the edges of the resulting network data set.
    If no further table is created, a basic node table with only a node id column will be automatically created by using unique values from the edges source and target columns.
    """

    _module_type_name = "assemble.network_graph"

    def create_inputs_schema(
        self,
    ) -> ValueMapSchema:
        inputs: Mapping[str, Any] = {
            "graph_type": {
                "type": "string",
                "type_config": {"allowed_strings": ALLOWED_GRAPH_TYPE_STRINGS},
                "doc": f"The type of graph that will be created. Allowed values: {', '.join(ALLOWED_GRAPH_TYPE_STRINGS)}.",
                "optional": False,
            },
            "edges": {
                "type": "table",
                "doc": "A table that contains the edges data.",
                "optional": False,
            },
            "source_column": {
                "type": "string",
                "doc": "The name of the source column name in the edges table.",
                "optional": False,
                "default": DEFAULT_SOURCE_COLUMN_NAME,
            },
            "target_column": {
                "type": "string",
                "doc": "The name of the target column name in the edges table.",
                "optional": False,
                "default": DEFAULT_TARGET_COLUMN_NAME,
            },
            "nodes": {
                "type": "table",
                "doc": "A table that contains the nodes data.",
                "optional": True,
            },
            "node_id_column": {
                "type": "string",
                "doc": "The name of the node-table column that contains the node identifier (used in the edges table), or the node id column name that will be used in case no 'nodes' table was provided.",
                "optional": False,
                "default": DEFAULT_NODE_ID_COLUMN_NAME,
            },
        }
        return inputs

    def create_outputs_schema(
        self,
    ) -> ValueMapSchema:
        outputs: Mapping[str, Any] = {
            "network_graph": {"type": "network_graph", "doc": "The network graph data."}
        }
        return outputs

    def process(self, inputs: ValueMap, outputs: ValueMap) -> None:

        graph_type_str = inputs.get_value_data("graph_type")
        graph_type = GraphType(graph_type_str)

        # the most important column is the id column, which is the only one that we absolutely need to have
        node_id_column_name = inputs.get_value_data("node_id_column")

        if not node_id_column_name:
            raise KiaraProcessingException("No node id column name provided.")

        # process nodes
        nodes = inputs.get_value_obj("nodes")

        if nodes.is_set:

            nodes_table: Union[KiaraTable, None] = nodes.data
            assert nodes_table is not None

            nodes_column_names = nodes_table.column_names

            if node_id_column_name not in nodes_column_names:
                raise KiaraProcessingException(
                    f"Could not find id column '{node_id_column_name}' in the nodes table. Please specify a valid column name manually, using one of: {', '.join(nodes_column_names)}"
                )

        else:
            nodes_table = None

        edges = inputs.get_value_obj("edges")
        edges_table: KiaraTable = edges.data
        edges_source_column_name = inputs.get_value_data("source_column")
        edges_target_column_name = inputs.get_value_data("target_column")

        if not edges_source_column_name:
            raise KiaraProcessingException("No source column name provided.")
        if not edges_target_column_name:
            raise KiaraProcessingException("No target column name provided.")

        edges_column_names = edges_table.column_names

        if edges_source_column_name not in edges_column_names:
            raise KiaraProcessingException(
                f"Edges table does not contain source column '{edges_source_column_name}'. Choose one of: {', '.join(edges_column_names)}."
            )
        if edges_target_column_name not in edges_column_names:
            raise KiaraProcessingException(
                f"Edges table does not contain target column '{edges_target_column_name}'. Choose one of: {', '.join(edges_column_names)}."
            )

        network_graph = NetworkGraph.create_from_tables(
            graph_type=graph_type,
            edges_table=edges_table,
            nodes_table=nodes_table,
            source_column_name=edges_source_column_name,
            target_column_name=edges_target_column_name,
            node_id_column_name=node_id_column_name,
        )

        outputs.set_value("network_graph", network_graph)

class AssignNetworkWeight(KiaraModule):
    """Assigns data for 'strength' or 'weight' of edges, and whether this is considered positive or negative. This can either be taken from a named weight column in the edges table, or calculated by aggregrating parallel edges where edge weight is assigned a weight of 1. If this module is not run, no weighted calculations will be run in future modules."""

    _module_type_name = "tropy.assign_network_weight"

    def create_inputs_schema(self):
        return {
            "network_graph": {
                "type": "network_graph",
                "doc": "The network graph for weights.",
            },
            "weight_column_name": {
                "type": "string",
                "default": "",
                "doc": "The name of the column in the edge table containing data for the 'weight' or 'strength' of an edge. If there is a column already named 'weight', this will be automatically selected. If otherwise left empty, weighted degree is calculated by aggregrating parallel edges where edge weight is assigned a weight of 1.",
            },
        }

    def create_outputs_schema(self):
        return {
            "weighted_network": {
                "type": "network_graph",
                "doc": "Updated network graph with weighted edges.",
            },
        }

    def process(self, inputs: ValueMap, outputs: ValueMap):

        import networkx as nx
        import pandas as pd

        from kiara_plugin.tropy.models import NetworkGraph

        edges = inputs.get_value_obj("network_graph")
        weight_meaning = inputs.get_value_data("weighted_meaning")
        weight_name = inputs.get_value_data("weight_column_name")

        network_data: NetworkGraph = edges.data

        G = network_data.as_networkx_graph()

        if weight_name == "":
                MG = network_data.as_networkx_graph()

                graph = nx.DiGraph()
                for u, v, data in MG.edges(data=True):
                    w = data["weight"] if "weight" in data else 1
                    if graph.has_edge(u, v):
                        graph[u][v]["weight"] += w
                    else:
                        graph.add_edge(u, v, weight=w)

                edge_weight = nx.get_edge_attributes(graph, "weight")
                nx.set_edge_attributes(G, edge_weight, "weight")

        if weight_name != "":
                edge_weight = nx.get_edge_attributes(G, weight_name)
                for u, v, key in edge_weight:
                    nx.set_edge_attributes(G, edge_weight, "weight")
        
        weighted_network = NetworkGraph.create_from_networkx_graph(G)

        outputs.set_values(weighted_network=weighted_network)