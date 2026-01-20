from ware_ops_algos.algorithms import LocalSearchBatching, NearestNeighbourhoodRouting, RandomBatching
from ware_ops_algos.domain_models import Resources, LayoutData, Articles
from ware_ops_pipes.pipelines.templates.template_1 import BatchedPickListGeneration
from ware_ops_pipes.utils.io_helpers import load_pickle


class LSBatchingNNRand(BatchedPickListGeneration):
    abstract = False

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
                                      routing_class=NearestNeighbourhoodRouting,
                                      routing_class_kwargs=routing_kwargs,
                                      start_batching_class=RandomBatching,
                                      time_limit=240)
        return batcher

