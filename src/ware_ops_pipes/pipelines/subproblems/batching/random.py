from ware_ops_algos.algorithms import RandomBatching
from ware_ops_algos.domain_models import Resources, Articles
from ware_ops_pipes.pipelines.templates.cosy_template import MultiOrderBatching
from ware_ops_pipes.pipelines.io_helpers import load_pickle


class Random(MultiOrderBatching):
    abstract = False
    algo_cls = RandomBatching

    def get_inited_batcher(self):
        articles: Articles = load_pickle(self.input()["instance"]["articles"].path)
        resources: Resources = load_pickle(self.input()["instance"]["resources"].path)
        batcher = RandomBatching(
            pick_cart=resources.resources[0].pick_cart,
            articles=articles,
        )
        return batcher

