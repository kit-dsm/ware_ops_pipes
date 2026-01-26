from ware_ops_algos.algorithms import MinMinItemAssignment
from ware_ops_algos.domain_models import StorageLocations, LayoutData
from ware_ops_pipes.pipelines.templates.template_1 import AbstractItemAssignment
from ware_ops_pipes.utils.io_helpers import load_pickle


class MinMinIA(AbstractItemAssignment):
    abstract = False

    def get_inited_item_assigner(self):
        storage_locations: StorageLocations = load_pickle(self.input()["instance"]["storage"].path)
        layout: LayoutData = load_pickle(self.input()["instance"]["layout"].path)
        layout_network = layout.layout_network

        item_assigner = MinMinItemAssignment(
            storage_locations=storage_locations,
            distance_matrix=layout_network.distance_matrix,
            start_node=layout_network.start_node
        )
        return item_assigner
