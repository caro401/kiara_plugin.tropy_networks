# -*- coding: utf-8 -*-
from kiara.api import KiaraModule

KIARA_METADATA = {
    "authors": [
        {"name": "Caitlin Burge", "email": "caitlin.burge@uni.lu"},
    ],
    "description": "Kiara modules for: network_analysis",
}


class CutPointsList(KiaraModule):
    """Create a list of nodes that are cut-points.
    Cut-points are any node in a network whose removal disconnects members of the network, creating one or more new distinct components.

    Uses networkx.articulation_points()
    https://networkx.org/documentation/stable/reference/algorithms/generated/networkx.algorithms.components.articulation_points.html"""

    _module_type_name = "tropy.create.cut_point_list"

    def create_inputs_schema(self):
        return {
            "network_graph": {
                "type": "network_graph",
                "doc": "The network graph being queried.",
            }
        }

    def create_outputs_schema(self):
        return {
            "network_result": {
                "type": "list",
                "doc": "A list of all nodes that are cut-points.",
            },
            "cut_network": {
                "type": "network_graph",
                "doc": "Updated network data with cut-point 'Yes' or 'No' assigned as a node attribute.",
            },
        }

    def process(self, inputs, outputs):

        from kiara_plugin.tropy.models import NetworkGraph

        edges = inputs.get_value_obj("network_graph")

        import networkx as nx

        network_data: NetworkGraph = edges.data

        G = network_data.as_networkx_graph()

        # TODO: I'm not sure what type the articulation points method returns, for my example
        # it seems to be some sort of numpy integer. So there might have to be a conversion
        # to 'normal' integers here?
        cutpoints = list(nx.articulation_points(G))

        cut_dict = {}
        for node in G:
            if node in cutpoints:
                cut_dict[node] = "Yes"
            else:
                cut_dict[node] = "No"

        nx.set_node_attributes(G, cut_dict, "Cut Point")

        attribute_network = NetworkGraph.create_from_networkx_graph(G)

        outputs.set_values(network_result=cutpoints, cut_network=attribute_network)
