# -*- coding: utf-8 -*-

"""This module contains the value type classes that are used in the ``kiara_plugin.tropy`` package.
"""
import atexit
import os
import shutil
import tempfile
from typing import TYPE_CHECKING, Any, ClassVar, Dict, List, Mapping, Type, Union

from rich.console import Group

from kiara.defaults import DEFAULT_PRETTY_PRINT_CONFIG
from kiara.utils.output import ArrowTabularWrap
from kiara_plugin.tabular.data_types.array import store_array
from kiara_plugin.tabular.data_types.tables import TablesType
from kiara_plugin.tabular.defaults import TABLE_COLUMN_SPLIT_MARKER
from kiara_plugin.tropy.defaults import EDGES_TABLE_NAME, NODES_TABLE_NAME
from kiara_plugin.tropy.models import NetworkGraph

if TYPE_CHECKING:
    from kiara.models.values.value import SerializedData, Value


class NetworkGraphType(TablesType):
    """Data that can be assembled into a network graph, incl. graph type and direction metadata.

    This data type extends the 'tables' type from the [kiara_plugin.tabular](https://github.com/DHARPA-Project/kiara_plugin.tabular) plugin, restricting the allowed tables to one called 'edges',
    and one called 'nodes'.
    """

    _data_type_name: ClassVar[str] = "network_graph"
    _cached_doc: ClassVar[Union[str, None]] = None

    @classmethod
    def python_class(cls) -> Type:
        result: Type[NetworkGraph] = NetworkGraph
        return result

    def parse_python_obj(self, data: Any) -> NetworkGraph:

        return data

    def _validate(cls, value: Any) -> None:
        if not isinstance(value, NetworkGraph):
            raise ValueError(
                f"Invalid type '{type(value)}': must be of 'NetworkGraph' (or a sub-class)."
            )

    def serialize(self, data: NetworkGraph) -> Union[None, str, "SerializedData"]:

        import pyarrow as pa

        for table_id, table in data.tables.items():
            if not table_id:
                raise Exception("table id must not be empty.")

            if TABLE_COLUMN_SPLIT_MARKER in table_id:
                raise Exception(
                    f"table id must not contain '{TABLE_COLUMN_SPLIT_MARKER}"
                )

        temp_f = tempfile.mkdtemp()

        def cleanup():
            shutil.rmtree(temp_f, ignore_errors=True)

        atexit.register(cleanup)

        chunk_map: Dict[str, Any] = {}

        for table_id, table in data.tables.items():
            arrow_table = table.arrow_table
            for column_name in arrow_table.column_names:
                if not column_name:
                    raise Exception(
                        f"column name for table '{table_id}' is empty. This is not allowed."
                    )

                column: pa.Array = arrow_table.column(column_name)
                file_name = os.path.join(temp_f, column_name)
                store_array(
                    array_obj=column, file_name=file_name, column_name=column_name
                )
                chunk_map[f"{table_id}{TABLE_COLUMN_SPLIT_MARKER}{column_name}"] = {
                    "type": "file",
                    "file": file_name,
                    "codec": "raw",
                }

        chunk_map["graph_metadata"] = {
            "type": "inline-json",
            "inline_data": {
                "graph_type": data.graph_type,
                "source_column_name": data.source_column_name,
                "target_column_name": data.target_column_name,
                "node_id_column_name": data.node_id_column_name,
            },
            "codec": "json",
        }

        serialized_data = {
            "data_type": self.data_type_name,
            "data_type_config": self.type_config.model_dump(),
            "data": chunk_map,
            "serialization_profile": "feather_and_json",
            "metadata": {
                "environment": {},
                "deserialize": {
                    "python_object": {
                        "module_type": "load.network_graph",
                        "module_config": {
                            "value_type": "network_graph",
                            "target_profile": "python_object",
                            "serialization_profile": "feather_and_json",
                        },
                    }
                },
            },
        }

        from kiara.models.values.value import SerializationResult

        serialized = SerializationResult(**serialized_data)
        return serialized

    def pretty_print_as__terminal_renderable(
        self, value: "Value", render_config: Mapping[str, Any]
    ) -> Any:

        max_rows = render_config.get(
            "max_no_rows", DEFAULT_PRETTY_PRINT_CONFIG["max_no_rows"]
        )
        max_row_height = render_config.get(
            "max_row_height", DEFAULT_PRETTY_PRINT_CONFIG["max_row_height"]
        )
        max_cell_length = render_config.get(
            "max_cell_length", DEFAULT_PRETTY_PRINT_CONFIG["max_cell_length"]
        )

        half_lines: Union[int, None] = None
        if max_rows:
            half_lines = int(max_rows / 2)

        network_data: NetworkGraph = value.data

        result: List[Any] = [""]

        from rich import box
        from rich.table import Table as RichTable

        details_table = RichTable(show_header=False, box=box.SIMPLE)
        details_table.add_column("Property")
        details_table.add_column("Value")
        details_table.add_row("Graph Type", network_data.graph_type)
        details_table.add_row(
            "Edge source column name", network_data.source_column_name
        )
        details_table.add_row(
            "Edge target column name", network_data.target_column_name
        )
        details_table.add_row("Node ID column name", network_data.node_id_column_name)

        result.append(details_table)

        nodes_atw = ArrowTabularWrap(network_data.nodes.arrow_table)
        nodes_pretty = nodes_atw.as_terminal_renderable(
            rows_head=half_lines,
            rows_tail=half_lines,
            max_row_height=max_row_height,
            max_cell_length=max_cell_length,
        )
        result.append(f"[b]{NODES_TABLE_NAME}[/b]")
        result.append(nodes_pretty)

        edges_atw = ArrowTabularWrap(network_data.edges.arrow_table)
        edges_pretty = edges_atw.as_terminal_renderable(
            rows_head=half_lines,
            rows_tail=half_lines,
            max_row_height=max_row_height,
            max_cell_length=max_cell_length,
        )
        result.append(f"[b]{EDGES_TABLE_NAME}[/b]")
        result.append(edges_pretty)

        return Group(*result)
