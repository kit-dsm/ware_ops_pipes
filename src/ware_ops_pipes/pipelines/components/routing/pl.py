from ware_ops_algos.algorithms import PickListRouting
from ware_ops_algos.domain_models import WarehouseInfo, Resource

from ware_ops_pipes.pipelines.templates.template_1 import PickerRouting
from ware_ops_pipes.utils.io_helpers import load_pickle


class PLRouting(PickerRouting):
    abstract = False

    def _get_inited_router(self):
        resources = self._load_resources()
        layout = self._load_layout()
        layout_network = layout.layout_network
        warehouse_info: WarehouseInfo = load_pickle(self.input()["instance"]["warehouse_info"].path)
        current_picker: Resource = warehouse_info.current_picker
        print("Routing received picker: ", current_picker.id)
        router = PickListRouting(
            start_node=layout_network.start_node,
            end_node=layout_network.end_node,
            closest_node_to_start=layout_network.closest_node_to_start,
            min_aisle_position=layout_network.min_aisle_position,
            max_aisle_position=layout_network.max_aisle_position,
            distance_matrix=layout_network.distance_matrix,
            predecessor_matrix=layout_network.predecessor_matrix,
            picker=[current_picker],
            gen_tour=True,
            gen_item_sequence=True,
            node_list=layout_network.node_list,
            node_to_idx={node: idx for idx, node in enumerate(list(layout_network.graph.nodes))},
            idx_to_node={idx: node for idx, node in enumerate(list(layout_network.graph.nodes))},
            fixed_depot=False  # todo needs to come from config
        )

        return router




