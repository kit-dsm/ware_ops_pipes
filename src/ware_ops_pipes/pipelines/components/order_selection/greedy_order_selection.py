from ware_ops_algos.algorithms.order_selection import GreedyOrderSelection

from ware_ops_pipes.pipelines.templates.template_1 import AbstractOrderSelection


class GreedyOS(AbstractOrderSelection):
    abstract = False

    def get_inited_order_selector(self):
        order_selector = GreedyOrderSelection()
        return order_selector
