# -*- coding: utf-8 -*-
DEFAULT_UNWEIGHTED_NODE_DEGREE_COLUMN_NAME = "_degree_unweighted"
DEFAULT_WEIGHTED_NODE_DEGREE_COLUMN_NAME = "_degree_weighted_"
DEFAULT_UNWEIGHTED_BETWEENNESS_COLUMN_NAME = "_betweenness"
DEFAULT_WEIGHTED_BETWEENNESS_COLUMN_NAME = "_betweenness_weighted_"

UNWEIGHTED_NODE_DEGREE_TEXT = (
    """The degree of a node is the number of edges connected to the node."""
)
WEIGHTED_NODE_DEGREE_TEXT = """The degree of a node is the number of edges connected to the node. The weight of the edges is taken into account."""
UNWEIGHTED_NODE_BETWEENNESS_TEXT = """The betweenness centrality for a node v is the fraction of all shortest paths that pass through v."""
WEIGHTED_NODE_BETWEENNESS_TEXT = """The betweenness centrality for a node v is the fraction of all shortest paths that pass through v. The weight of the edges is taken into account."""
