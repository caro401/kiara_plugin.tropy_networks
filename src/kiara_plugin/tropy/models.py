# -*- coding: utf-8 -*-

"""This module contains the metadata (and other) models that are used in the ``kiara_plugin.tropy`` package.

Those models are convenience wrappers that make it easier for *kiara* to find, create, manage and version metadata -- but also
other type of models -- that is attached to data, as well as *kiara* modules.

Metadata models must be a sub-class of [kiara.metadata.MetadataModel][kiara.metadata.MetadataModel]. Other models usually
sub-class a pydantic BaseModel or implement custom base classes.
"""
from typing import (
    TYPE_CHECKING,
    Any,
    ClassVar,
    Iterable,
    Literal,
    TypeVar,
    Union,
)

from pydantic import BaseModel, Field

from kiara.exceptions import KiaraException
from kiara.models.values.value import Value
from kiara.models.values.value_metadata import ValueMetadata
from kiara_plugin.tabular.models.table import KiaraTable
from kiara_plugin.tabular.models.tables import KiaraTables
from kiara_plugin.tropy.defaults import (
    DEFAULT_NODE_ID_COLUMN_NAME,
    DEFAULT_SOURCE_COLUMN_NAME,
    DEFAULT_TARGET_COLUMN_NAME,
    EDGES_TABLE_NAME,
    NODES_TABLE_NAME,
    GraphType,
)

if TYPE_CHECKING:
    import networkx as nx
    import pyarrow as pa


NETWORKX_GRAPH_TYPE = TypeVar("NETWORKX_GRAPH_TYPE", bound="nx.Graph")


class NetworkGraph(KiaraTables):
    """A wrapper class to access and query network graph data."""

    _kiara_model_id: ClassVar = "instance.network_graph"

    @classmethod
    def create_from_kiara_tables(
        cls,
        graph_type: GraphType,
        tables: KiaraTables,
        source_column_name: str = DEFAULT_SOURCE_COLUMN_NAME,
        target_column_name: str = DEFAULT_TARGET_COLUMN_NAME,
        node_id_column_name: str = DEFAULT_NODE_ID_COLUMN_NAME,
    ) -> "NetworkGraph":
        if EDGES_TABLE_NAME not in tables.tables.keys():
            raise KiaraException(
                f"Can't import network data: no '{EDGES_TABLE_NAME}' table found"
            )

        if NODES_TABLE_NAME not in tables.tables.keys():
            nodes_table: Union[KiaraTable, None] = None
        else:
            nodes_table = tables.tables[NODES_TABLE_NAME]

        return cls.create_from_tables(
            graph_type=graph_type,
            edges_table=tables.tables[EDGES_TABLE_NAME],
            nodes_table=nodes_table,
            source_column_name=source_column_name,
            target_column_name=target_column_name,
            node_id_column_name=node_id_column_name,
        )

    @classmethod
    def create_from_tables(
        cls,
        graph_type: GraphType,
        edges_table: Any,
        nodes_table: Union[Any, None] = None,
        source_column_name: str = DEFAULT_SOURCE_COLUMN_NAME,
        target_column_name: str = DEFAULT_TARGET_COLUMN_NAME,
        node_id_column_name: str = DEFAULT_NODE_ID_COLUMN_NAME,
    ) -> "NetworkGraph":

        edges_table = KiaraTable.create_table(edges_table)
        nodes_table = KiaraTable.create_table(nodes_table) if nodes_table else None

        edges_columns = edges_table.column_names
        if source_column_name not in edges_columns:
            raise Exception(
                f"Invalid 'network_data' value: 'edges' table does not contain a '{source_column_name}' column. Available columns: {', '.join(edges_columns)}."
            )
        if target_column_name not in edges_columns:
            raise Exception(
                f"Invalid 'network_data' value: 'edges' table does not contain a '{target_column_name}' column. Available columns: {', '.join(edges_columns)}."
            )

        if not nodes_table:
            import duckdb

            edges: pa.Table = edges_table.arrow_table  # noqa

            sql_query = f"""
            SELECT DISTINCT combined.{node_id_column_name}
            FROM (
                 SELECT {source_column_name} AS {node_id_column_name} FROM edges
                 UNION
                 SELECT {target_column_name} AS {node_id_column_name} FROM edges
            ) AS combined
            ORDER BY combined.{node_id_column_name}
            """

            con = duckdb.connect()
            result = con.execute(sql_query)
            nodes_table_arrow = result.arrow()
            nodes_table = KiaraTable.create_table(nodes_table_arrow)
        else:
            nodes_columns = nodes_table.column_names
            if node_id_column_name not in nodes_columns:
                raise Exception(
                    f"Invalid 'network_data' value: 'nodes' table does not contain a '{node_id_column_name}' column. Available columns: {', '.join(nodes_columns)}."
                )

        graph = NetworkGraph(
            graph_type=graph_type.value,
            source_column_name=source_column_name,
            target_column_name=target_column_name,
            node_id_column_name=node_id_column_name,
            tables={EDGES_TABLE_NAME: edges_table, NODES_TABLE_NAME: nodes_table},
        )

        return graph

    @classmethod
    def create_from_networkx_graph(
        cls,
        graph: NETWORKX_GRAPH_TYPE,
        source_column_name: str = DEFAULT_SOURCE_COLUMN_NAME,
        target_column_name: str = DEFAULT_TARGET_COLUMN_NAME,
        node_id_column_name: str = DEFAULT_NODE_ID_COLUMN_NAME,
    ) -> "NetworkGraph":
        """Create a `NetworkGraph` instance from a networkx Graph object."""

        import networkx as nx
        import pandas as pd

        if isinstance(graph, nx.MultiDiGraph):
            graph_type = GraphType.DIRECTED_MULTI
        elif isinstance(graph, nx.MultiGraph):
            graph_type = GraphType.UNDIRECTED_MULTI
        elif isinstance(graph, nx.DiGraph):
            graph_type = GraphType.DIRECTED
        elif isinstance(graph, nx.Graph):
            graph_type = GraphType.UNDIRECTED
        else:
            raise KiaraException(f"Invalid graph type: {type(graph)}")

        node_dict = dict(graph.nodes(data=True))
        nodes_data = pd.DataFrame.from_dict(node_dict, orient="index")
        nodes_data = nodes_data.reset_index()
        nodes_data = nodes_data.rename(columns={"index": node_id_column_name})
        nodes_table = KiaraTable.create_table(nodes_data)

        edges_df = nx.to_pandas_edgelist(
            graph, source=source_column_name, target=target_column_name
        )
        edges_table = KiaraTable.create_table(edges_df)

        return cls.create_from_tables(
            graph_type=graph_type,
            edges_table=edges_table,
            nodes_table=nodes_table,
            source_column_name="source",
            target_column_name="target",
        )

    source_column_name: str = Field(
        description="The name of the column in the edges table that contains the source node id."
    )
    target_column_name: str = Field(
        description="The name of the column in the edges table that contains the target node id."
    )
    node_id_column_name: str = Field(
        description="The name of the column in the nodes table that contains the node id."
    )
    graph_type: Literal[
        "directed", "undirected", "directed_multi", "undirected_multi"
    ] = Field(
        description="The type of the graph (directed, undirected, directed_multi, undirected_multi)."
    )

    @property
    def edges(self) -> "KiaraTable":
        """Return the edges table."""

        return self.tables[EDGES_TABLE_NAME]

    @property
    def nodes(self) -> "KiaraTable":
        """Return the nodes table."""

        return self.tables[NODES_TABLE_NAME]

    @property
    def num_nodes(self):
        """Return the number of nodes in the network data."""

        return self.nodes.num_rows

    @property
    def num_edges(self):
        """Return the number of edges in the network data."""

        return self.edges.num_rows

    def query(self, sql_query: str) -> "pa.Table":
        """Query the edges and nodes tables using SQL.

        The table names to use in the query are 'edges' and 'nodes'.
        """

        import duckdb

        con = duckdb.connect()
        edges = self.edges.arrow_table  # noqa
        nodes = self.nodes.arrow_table  # noqa

        result = con.execute(sql_query)
        return result.arrow()

    def as_networkx_graph(
        self,
    ) -> Union["nx.Graph", "nx.DiGraph", "nx.MultiGraph", "nx.MultiDiGraph"]:
        """Return the network data as a networkx graph object."""

        import networkx as nx

        if self.graph_type == GraphType.DIRECTED.value:
            graph_type = nx.DiGraph
        elif self.graph_type == GraphType.UNDIRECTED.value:
            graph_type = nx.Graph
        elif self.graph_type == GraphType.DIRECTED_MULTI.value:
            graph_type = nx.MultiDiGraph
        elif self.graph_type == GraphType.UNDIRECTED_MULTI.value:
            graph_type = nx.MultiGraph
        else:
            raise KiaraException("Invalid graph type: {self.graph_type}")

        graph = graph_type()

        # this is all fairly wateful in terms of memory, but since we are using networkx for everything
        # now, it probably doesn't matter much

        # Add nodes
        nodes_df = self.nodes.arrow_table.to_pandas()
        for idx, row in nodes_df.iterrows():
            graph.add_node(row[self.node_id_column_name], **row.to_dict())

        # Add edges
        edges_df = self.edges.arrow_table.to_pandas()
        for idx, row in edges_df.iterrows():
            graph.add_edge(
                row[self.source_column_name],
                row[self.target_column_name],
                **row.to_dict(),
            )

        return graph


class GraphProperties(BaseModel):
    """Properties of graph data, if interpreted as a specific graph type."""

    number_of_edges: int = Field(description="The number of edges.")
    parallel_edges: int = Field(
        description="The number of parallel edges (if 'multi' graph type).", default=0
    )


class NetworkGraphProperties(ValueMetadata):
    """Network data stats."""

    _metadata_key: ClassVar[str] = "network_data"

    number_of_nodes: int = Field(description="Number of nodes in the network graph.")
    num_edges: int = Field(description="Number of edges in the network graph.")

    @classmethod
    def retrieve_supported_data_types(cls) -> Iterable[str]:
        return ["network_graph"]

    @classmethod
    def create_value_metadata(cls, value: Value) -> "NetworkGraphProperties":

        network_data: NetworkGraph = value.data

        num_rows = network_data.num_nodes
        num_edges = network_data.num_edges

        result = cls(
            number_of_nodes=num_rows,
            num_edges=num_edges,
        )
        return result
