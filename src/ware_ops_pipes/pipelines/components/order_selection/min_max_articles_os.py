from ware_ops_algos.algorithms.order_selection import MinMaxArticlesCobotSelection
from ware_ops_algos.domain_models import Resources

from ware_ops_pipes.pipelines.templates.template_1 import AbstractOrderSelection
from ware_ops_pipes.utils.io_helpers import load_pickle


class MinMaxArticlesOS(AbstractOrderSelection):
    abstract = False

    def get_inited_order_selector(self):
        resources: Resources = load_pickle(self.input()["instance"]["resources"].path)
        order_selector = MinMaxArticlesCobotSelection(resources.resources[0])
        return order_selector
