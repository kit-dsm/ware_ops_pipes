from ware_ops_algos.algorithms import TourPlanningState, PickListRouting
from ware_ops_algos.algorithms.order_selection import TimeIndexedMinConflictSelection
from ware_ops_algos.domain_models import WarehouseInfo, Resources, LayoutData, Resource

from ware_ops_pipes.pipelines.templates.template_1 import AbstractOrderSelection
from ware_ops_pipes.utils.io_helpers import load_pickle


class MinAisleConflictsOS(AbstractOrderSelection):
    abstract = False

    def get_inited_order_selector(self):
        resources: Resources = load_pickle(self.input()["instance"]["resources"].path)
        layout: LayoutData = load_pickle(self.input()["instance"]["layout"].path)
        layout_network = layout.layout_network
        warehouse_info: WarehouseInfo = load_pickle(self.input()["instance"]["warehouse_info"].path)
        tours: list[TourPlanningState] = warehouse_info.active_tours
        current_picker: Resource = warehouse_info.current_picker

        routing_kwargs = {"start_node": layout_network.start_node,
                          "end_node": layout_network.end_node,
                          "closest_node_to_start": layout_network.closest_node_to_start,
                          "min_aisle_position": layout_network.min_aisle_position,
                          "max_aisle_position": layout_network.max_aisle_position,
                          "distance_matrix": layout_network.distance_matrix,
                          "predecessor_matrix": layout_network.predecessor_matrix,
                          "picker": [current_picker],
                          "gen_tour": True,
                          "gen_item_sequence": True,
                          "fixed_depot": True,
                          "node_list": layout_network.node_list,
                          "node_to_idx": {node: idx for idx, node in enumerate(
                              list(layout_network.graph.nodes))},
                          "idx_to_node": {idx: node for idx, node in enumerate(
                              list(layout_network.graph.nodes))}
                          }

        order_selector = TimeIndexedMinConflictSelection(
            active_tours=tours,
            resource=current_picker,
            resources=resources.resources,
            picker_position=current_picker.current_location,
            distance_matrix=layout_network.distance_matrix,
            routing_class=PickListRouting,
            routing_class_kwargs=routing_kwargs)
        return order_selector
