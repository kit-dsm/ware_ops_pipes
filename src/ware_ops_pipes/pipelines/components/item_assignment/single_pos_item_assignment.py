from ware_ops_algos.algorithms import SinglePositionItemAssignment, RatliffRosenthalRouting
from ware_ops_algos.domain_models import StorageLocations, LayoutData, Resources
from ware_ops_pipes.pipelines.templates.template_1 import AbstractItemAssignment
from ware_ops_pipes.utils.io_helpers import load_pickle


class SinglePosIA(AbstractItemAssignment):
    abstract = False

    def get_inited_item_assigner(self):
        storage_locations: StorageLocations = load_pickle(self.input()["instance"]["storage"].path)
        layout: LayoutData = load_pickle(self.input()["instance"]["layout"].path)
        resources: Resources = load_pickle(self.input()["instance"]["resources"].path)
        layout_network = layout.layout_network
        graph_params = layout.graph_data
        routing_kwargs = {"start_node": layout_network.start_node,
                          "end_node": layout_network.end_node,
                          "closest_node_to_start": layout_network.closest_node_to_start,
                          "min_aisle_position": layout_network.min_aisle_position,
                          "max_aisle_position": layout_network.max_aisle_position,
                          "distance_matrix": layout_network.distance_matrix,
                          "predecessor_matrix": layout_network.predecessor_matrix,
                          "picker": resources.resources,
                          "n_aisles": graph_params.n_aisles,
                          "n_pick_locations": graph_params.n_pick_locations,
                          "dist_aisle": graph_params.dist_aisle,
                          "dist_pick_locations": graph_params.dist_pick_locations,
                          "dist_aisle_location": graph_params.dist_bottom_to_pick_location,
                          "dist_start": graph_params.dist_start,
                          "dist_end": graph_params.dist_end,
                          "gen_tour": False,
                          "gen_item_sequence": False,
                          }
        item_assigner = SinglePositionItemAssignment(
            storage_locations=storage_locations,
            distance_matrix=layout_network.distance_matrix,
            routing_class=RatliffRosenthalRouting,
            routing_class_kwargs=routing_kwargs
        )
        return item_assigner
