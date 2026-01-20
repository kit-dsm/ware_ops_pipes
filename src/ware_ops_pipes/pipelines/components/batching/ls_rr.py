from ware_ops_algos.algorithms import LocalSearchBatching, RatliffRosenthalRouting, OrderNrFifoBatching
from ware_ops_algos.domain_models import Resources, LayoutData, Articles
from ware_ops_pipes.pipelines.templates.template_1 import BatchedPickListGeneration
from ware_ops_pipes.utils.io_helpers import load_pickle


class LSBatchingRR(BatchedPickListGeneration):
    abstract = False

    def get_inited_batcher(self):
        articles: Articles = load_pickle(self.input()["instance"]["articles"].path)
        resources: Resources = load_pickle(self.input()["instance"]["resources"].path)
        layout: LayoutData = load_pickle(self.input()["instance"]["layout"].path)
        layout_network = layout.layout_network
        graph_params = layout.graph_data
        routing_kwargs = {
            "start_node": layout_network.start_node,
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
            "gen_item_sequence": False
        }

        batcher = LocalSearchBatching(
                                      pick_cart=resources.resources[0].pick_cart,
                                      articles=articles,
                                      routing_class=RatliffRosenthalRouting,
                                      routing_class_kwargs=routing_kwargs,
                                      start_batching_class=OrderNrFifoBatching,
                                      time_limit=240)
        return batcher

