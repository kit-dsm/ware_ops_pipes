from ware_ops_algos.algorithms import GreedyItemAssignment
from ware_ops_algos.domain_models import StorageLocations
from ware_ops_pipes.pipelines.templates.template_1 import AbstractItemAssignment
from ware_ops_pipes.utils.io_helpers import load_pickle


class GreedyIA(AbstractItemAssignment):
    abstract = False

    def get_inited_item_assigner(self):
        storage: StorageLocations = load_pickle(self.input()["instance"]["storage"].path)
        item_assigner = GreedyItemAssignment(storage)
        return item_assigner
