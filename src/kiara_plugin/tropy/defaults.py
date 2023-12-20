# -*- coding: utf-8 -*-
from enum import Enum

EDGES_TABLE_NAME = "edges"
NODES_TABLE_NAME = "nodes"

DEFAULT_SOURCE_COLUMN_NAME = "source"
DEFAULT_TARGET_COLUMN_NAME = "target"

DEFAULT_NODE_ID_COLUMN_NAME = "node_id"


class GraphType(Enum):
    UNDIRECTED = "undirected"
    DIRECTED = "directed"
    DIRECTED_MULTI = "directed_multi"
    UNDIRECTED_MULTI = "undirected_multi"


ALLOWED_GRAPH_TYPE_STRINGS = [
    GraphType.DIRECTED.value,
    GraphType.UNDIRECTED.value,
    GraphType.DIRECTED_MULTI.value,
    GraphType.UNDIRECTED_MULTI.value,
]
