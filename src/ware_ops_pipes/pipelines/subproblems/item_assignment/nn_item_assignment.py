from ware_ops_algos.algorithms import NearestNeighborItemAssignment
from ware_ops_algos.domain_models import LayoutData, StorageLocations
from ware_ops_pipes.pipelines.templates.template_1 import AbstractItemAssignment
from ware_ops_pipes.pipelines.io_helpers import load_pickle


class NNIA(AbstractItemAssignment):
    abstract = False

    def get_inited_item_assigner(self):
        layout: LayoutData = load_pickle(self.input()["instance"]["layout"].path)
        storage: StorageLocations = load_pickle(self.input()["instance"]["storage"].path)
        layout_network = layout.layout_network
        item_assigner = NearestNeighborItemAssignment(
            storage,
            distance_matrix=layout_network.distance_matrix,
            start_node=layout_network.start_node
        )
        return item_assigner
