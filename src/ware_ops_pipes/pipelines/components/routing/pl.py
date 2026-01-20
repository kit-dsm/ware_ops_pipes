from ware_ops_algos.algorithms import PickListRouting
from ware_ops_pipes.pipelines.templates.template_1 import PickerRouting


class PLRouting(PickerRouting):
    abstract = False

    def _get_inited_router(self):
        resources = self._load_resources()
        layout = self._load_layout()
        layout_network = layout.layout_network
        router = PickListRouting(
            start_node=layout_network.start_node,
            end_node=layout_network.end_node,
            closest_node_to_start=layout_network.closest_node_to_start,
            min_aisle_position=layout_network.min_aisle_position,
            max_aisle_position=layout_network.max_aisle_position,
            distance_matrix=layout_network.distance_matrix,
            predecessor_matrix=layout_network.predecessor_matrix,
            picker=resources.resources,
            gen_tour=True,
            gen_item_sequence=True,
            node_list=layout_network.node_list,
            node_to_idx={node: idx for idx, node in enumerate(list(layout_network.graph.nodes))},
            idx_to_node={idx: node for idx, node in enumerate(list(layout_network.graph.nodes))}
        )

        return router




