from ware_ops_algos.algorithms import LocalSearchBatching, NearestNeighbourhoodRouting, RandomBatching
from ware_ops_algos.domain_models import Resources, LayoutData, Articles
from ware_ops_pipes.pipelines.templates.cosy_template import MultiOrderBatching, class_fingerprint_payload
from ware_ops_pipes.pipelines.io_helpers import load_pickle


class LSBatchingNNRand(MultiOrderBatching):
    abstract = False
    algo_cls = LocalSearchBatching

    start_batching_cls = RandomBatching
    routing_class = NearestNeighbourhoodRouting

    def config_fingerprint_payload(self) -> dict:
        return {
            "start_batching_cls": class_fingerprint_payload(self.start_batching_cls),
            "routing_class": class_fingerprint_payload(self.routing_class),
            "time_limit": self.pipeline_params.time_limit_sec
        }

    def get_inited_batcher(self):
        articles: Articles = load_pickle(self.input()["instance"]["articles"].path)
        resources: Resources = load_pickle(self.input()["instance"]["resources"].path)
        layout: LayoutData = load_pickle(self.input()["instance"]["layout"].path)
        layout_network = layout.layout_network
        routing_kwargs = {
            "start_node": layout_network.start_node,
            "end_node": layout_network.end_node,
            "closest_node_to_start": layout_network.closest_node_to_start,
            "min_aisle_position": layout_network.min_aisle_position,
            "max_aisle_position": layout_network.max_aisle_position,
            "distance_matrix": layout_network.distance_matrix,
            "predecessor_matrix": layout_network.predecessor_matrix,
            "picker": resources.resources,
            "gen_tour": True,
            "gen_item_sequence": True,
            "node_list": layout_network.node_list,
            "node_to_idx": {node: idx for idx, node in enumerate(list(layout_network.graph.nodes))},
            "idx_to_node": {idx: node for idx, node in enumerate(list(layout_network.graph.nodes))}
        }

        batcher = LocalSearchBatching(#capacity=resources.resources[0].capacity,
                                      pick_cart=resources.resources[0].pick_cart,
                                      articles=articles,
                                      routing_class=self.routing_class,
                                      routing_class_kwargs=routing_kwargs,
                                      start_batching_class=self.start_batching_cls,
                                      time_limit=self.pipeline_params.time_limit_sec)
        return batcher

