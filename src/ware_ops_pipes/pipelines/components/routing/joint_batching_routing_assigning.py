from ware_ops_algos.algorithms import ExactTSPBatchingAndRoutingDistance
from ware_ops_pipes.pipelines.templates.template_1 import CombinedBR


class CombinedBatchingRoutingAssigning(CombinedBR):
    abstract = False

    def _get_inited_router(self):
        resources = self._load_resources()
        layout = self._load_layout()
        layout_network = layout.layout_network
        graph = layout_network.graph

        router = ExactTSPBatchingAndRoutingDistance(
            start_node=layout_network.start_node,
            end_node=layout_network.end_node,
            distance_matrix=layout_network.distance_matrix,
            predecessor_matrix=layout_network.predecessor_matrix,
            picker=resources.resources,
            gen_tour=True,
            gen_item_sequence=True,
            big_m=1000,
            time_limit=3600,
            node_list=layout_network.node_list,
            node_to_idx={node: idx for idx, node in enumerate(list(graph.nodes))},
            idx_to_node={idx: node for idx, node in enumerate(list(graph.nodes))},
        )
        return router



