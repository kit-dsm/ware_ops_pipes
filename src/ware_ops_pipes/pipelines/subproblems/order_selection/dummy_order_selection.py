from ware_ops_algos.algorithms.order_selection import DummyOrderSelection
from ware_ops_pipes.pipelines.templates.template_1 import AbstractOrderSelection


class DummyOS(AbstractOrderSelection):
    abstract = False

    def get_inited_order_selector(self):
        order_selector = DummyOrderSelection()
        return order_selector
