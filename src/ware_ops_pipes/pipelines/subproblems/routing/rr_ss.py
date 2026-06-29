from ware_ops_algos.algorithms import RatliffRosenthalScatteredRouting
from ware_ops_pipes.pipelines.templates.cosy_template import CombinedIAR


class RatliffRosenthalNF(CombinedIAR):
    abstract = False

    def _get_inited_router(self):
        resources = self._load_resources()
        storage_locations = self._load_storage()
        layout = self._load_layout()
        graph_params = layout.graph_data
        layout_network = layout.layout_network

        rr_routing = RatliffRosenthalScatteredRouting(
            start_node=layout.graph_data.start_connection_point,
            end_node=layout_network.end_node,
            closest_node_to_start=layout_network.closest_node_to_start,
            min_aisle_position=layout_network.min_aisle_position,
            max_aisle_position=layout_network.max_aisle_position,
            picker=resources.resources,
            n_aisles=graph_params.n_aisles,
            n_pick_locations=graph_params.n_pick_locations,
            dist_aisle=graph_params.dist_aisle,
            dist_pick_locations=graph_params.dist_pick_locations,
            dist_aisle_location=graph_params.dist_bottom_to_pick_location,
            dist_start=graph_params.dist_start,
            dist_end=graph_params.dist_end,
            storage_locations=storage_locations
        )

        return rr_routing



