from ware_ops_algos.algorithms import MinMaxItemAssignment
from ware_ops_algos.domain_models import StorageLocations, LayoutData
from ware_ops_pipes.pipelines.templates.cosy_template import AbstractItemAssignment
from ware_ops_pipes.pipelines.io_helpers import load_pickle


class MinMaxIA(AbstractItemAssignment):
    abstract = False

    def get_inited_item_assigner(self):
        storage_locations: StorageLocations = load_pickle(self.input()["instance"]["storage"].path)
        layout: LayoutData = load_pickle(self.input()["instance"]["layout"].path)
        layout_network = layout.layout_network

        item_assigner = MinMaxItemAssignment(
            storage_locations=storage_locations,
            distance_matrix=layout_network.distance_matrix,
            start_node=layout_network.start_node
        )
        return item_assigner
