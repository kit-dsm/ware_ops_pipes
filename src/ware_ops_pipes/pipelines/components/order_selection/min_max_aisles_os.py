from ware_ops_algos.algorithms.order_selection import MinMaxAisleOrderSelection
from ware_ops_algos.domain_models import WarehouseInfo, Resources

from ware_ops_pipes.pipelines.templates.template_1 import AbstractOrderSelection
from ware_ops_pipes.utils.io_helpers import load_pickle


class MinMaxAislesOS(AbstractOrderSelection):
    abstract = False

    def get_inited_order_selector(self):
        warehouse_info: WarehouseInfo = load_pickle(self.input()["instance"]["warehouse_info"].path)
        congestion = warehouse_info.congestion_rate
        resources: Resources = load_pickle(self.input()["instance"]["resources"].path)
        order_selector = MinMaxAisleOrderSelection(congestion, resources.resources[0])
        return order_selector
