from ware_ops_algos.algorithms import ExactCombinedBatchingRouting
from ware_ops_pipes.pipelines.templates.cosy_template import CombinedBR


class CombinedBatchingRoutingAssigning(CombinedBR):
    abstract = False
    algo_cls = ExactCombinedBatchingRouting

    def _get_inited_router(self):
        resources = self._load_resources()
        layout = self._load_layout()
        layout_network = layout.layout_network
        graph = layout_network.graph

        router = ExactCombinedBatchingRouting(
            start_node=layout_network.start_node,
            end_node=layout_network.end_node,
            distance_matrix=layout_network.distance_matrix,
            predecessor_matrix=layout_network.predecessor_matrix,
            picker=resources.resources,
            gen_tour=False,
            gen_item_sequence=True,
            time_limit=self.pipeline_params.time_limit_sec,
            node_list=layout_network.node_list,
            node_to_idx={node: idx for idx, node in enumerate(list(graph.nodes))},
            idx_to_node={idx: node for idx, node in enumerate(list(graph.nodes))},
        )
        return router



