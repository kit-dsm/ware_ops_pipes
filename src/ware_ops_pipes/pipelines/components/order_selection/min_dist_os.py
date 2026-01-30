from ware_ops_algos.algorithms.order_selection import MinDistOrderSelection
from ware_ops_algos.domain_models import WarehouseInfo, Resources, LayoutData, Resource

from ware_ops_pipes.pipelines.templates.template_1 import AbstractOrderSelection
from ware_ops_pipes.utils.io_helpers import load_pickle


class MinDistOS(AbstractOrderSelection):
    abstract = False

    def get_inited_order_selector(self):
        resources: Resources = load_pickle(self.input()["instance"]["resources"].path)
        layout: LayoutData = load_pickle(self.input()["instance"]["layout"].path)
        warehouse_info: WarehouseInfo = load_pickle(self.input()["instance"]["warehouse_info"].path)
        current_picker: Resource = warehouse_info.current_picker
        dima = layout.layout_network.distance_matrix
        order_selector = MinDistOrderSelection(current_picker.current_location, dima)
        return order_selector
