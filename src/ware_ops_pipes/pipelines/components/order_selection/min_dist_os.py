from ware_ops_algos.algorithms.order_selection import MinDistOrderSelection
from ware_ops_algos.domain_models import WarehouseInfo, Resources, LayoutData

from ware_ops_pipes.pipelines.templates.template_1 import AbstractOrderSelection
from ware_ops_pipes.utils.io_helpers import load_pickle


class MinDistOS(AbstractOrderSelection):
    abstract = False

    def get_inited_order_selector(self):
        resources: Resources = load_pickle(self.input()["instance"]["resources"].path)
        layout: LayoutData = load_pickle(self.input()["instance"]["layout"].path)
        dima = layout.layout_network.distance_matrix
        order_selector = MinDistOrderSelection(resources.resources[0].current_location, dima)
        return order_selector
