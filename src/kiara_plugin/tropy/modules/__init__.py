# -*- coding: utf-8 -*-
from typing import Any, Dict, Mapping, Type

import orjson

from kiara.exceptions import KiaraException
from kiara.models.values.value import SerializedData
from kiara.modules.included_core_modules.serialization import DeserializeValueModule
from kiara_plugin.tabular.defaults import TABLE_COLUMN_SPLIT_MARKER
from kiara_plugin.tropy.defaults import GraphType
from kiara_plugin.tropy.models import NetworkGraph

KIARA_METADATA = {
    "authors": [
        {"name": "Caitlin Burge", "email": "caitlin.burge@uni.lu"},
    ],
    "description": "Kiara modules for: tropy",
}


class DeserializeTableModule(DeserializeValueModule):

    _module_type_name = "load.network_graph"

    @classmethod
    def retrieve_supported_target_profiles(cls) -> Mapping[str, Type]:
        return {"python_object": NetworkGraph}

    @classmethod
    def retrieve_serialized_value_type(cls) -> str:
        return "network_graph"

    @classmethod
    def retrieve_supported_serialization_profile(cls) -> str:
        return "feather_and_json"

    def to__python_object(self, data: SerializedData, **config: Any):

        import pyarrow as pa

        tables: Dict[str, Any] = {}

        graph_metadata_chunks = data.get_serialized_data("graph_metadata")
        chunk = next(iter(graph_metadata_chunks.get_chunks(as_files=False)))

        graph_metadata = orjson.loads(chunk)
        graph_type_str = graph_metadata["graph_type"]
        graph_type = GraphType(graph_type_str)

        source_column_name = graph_metadata["source_column_name"]
        target_column_name = graph_metadata["target_column_name"]
        node_id_column_name = graph_metadata["node_id_column_name"]

        for column_id in data.get_keys():

            if column_id == "graph_metadata":
                continue

            if TABLE_COLUMN_SPLIT_MARKER not in column_id:
                raise KiaraException(
                    f"Invalid serialized 'network_graph' data, key must contain '{TABLE_COLUMN_SPLIT_MARKER}': {column_id}"
                )
            table_id, column_name = column_id.split(
                TABLE_COLUMN_SPLIT_MARKER, maxsplit=1
            )

            chunks = data.get_serialized_data(column_id)

            # TODO: support multiple chunks
            assert chunks.get_number_of_chunks() == 1
            files = list(chunks.get_chunks(as_files=True, symlink_ok=True))
            assert len(files) == 1

            file = files[0]
            with pa.memory_map(file, "r") as column_chunk:
                loaded_arrays: pa.Table = pa.ipc.open_file(column_chunk).read_all()
                column = loaded_arrays.column(column_name)
                tables.setdefault(table_id, {})[column_name] = column

        edges_table = tables["edges"]
        nodes_table = tables.get("nodes")

        network_graph = NetworkGraph.create_from_tables(
            graph_type=graph_type,
            edges_table=edges_table,
            nodes_table=nodes_table,
            source_column_name=source_column_name,
            target_column_name=target_column_name,
            node_id_column_name=node_id_column_name,
        )
        return network_graph
