from ware_ops_algos.algorithms.order_selection import MinAisleOrderSelection
from ware_ops_algos.domain_models import WarehouseInfo

from ware_ops_pipes.pipelines.templates.template_1 import AbstractOrderSelection
from ware_ops_pipes.utils.io_helpers import load_pickle


class MinSharedAislesOS(AbstractOrderSelection):
    abstract = False

    def get_inited_order_selector(self):
        warehouse_info: WarehouseInfo = load_pickle(self.input()["instance"]["warehouse_info"].path)
        congestion = warehouse_info.congestion_rate
        order_selector = MinAisleOrderSelection(congestion)
        return order_selector
