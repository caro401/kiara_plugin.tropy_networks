# -*- coding: utf-8 -*-
from typing import Any

from pydantic import Field

from kiara.exceptions import KiaraProcessingException
from kiara.models.filesystem import (
    KiaraFile,
)
from kiara.models.values.value import Value
from kiara.modules.included_core_modules.create_from import (
    CreateFromModule,
    CreateFromModuleConfig,
)
from kiara_plugin.tropy.models import NetworkGraph

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
    _module_type_name = "create.network_graph"
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

        source_file: KiaraFile = source_value.data
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
