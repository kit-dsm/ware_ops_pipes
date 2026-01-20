from ware_ops_algos.algorithms import OrderNrFifoBatching
from ware_ops_algos.domain_models import Resources, Articles
from ware_ops_pipes.pipelines.templates.template_1 import BatchedPickListGeneration
from ware_ops_pipes.utils.io_helpers import load_pickle


class OrderNrFiFo(BatchedPickListGeneration):
    abstract = False

    def get_inited_batcher(self):
        articles: Articles = load_pickle(self.input()["instance"]["articles"].path)
        resources: Resources = load_pickle(self.input()["instance"]["resources"].path)
        batcher = OrderNrFifoBatching(
            pick_cart=resources.resources[0].pick_cart,
            articles=articles,
        )
        return batcher

