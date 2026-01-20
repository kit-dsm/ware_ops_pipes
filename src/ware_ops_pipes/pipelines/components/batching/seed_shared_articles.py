from ware_ops_algos.algorithms import SeedBatching, SeedCriteria, SimilarityMeasure
from ware_ops_algos.domain_models import Resources, LayoutData, Articles
from ware_ops_pipes.pipelines.templates.template_1 import BatchedPickListGeneration
from ware_ops_pipes.utils.io_helpers import load_pickle


class ClosestDepotMaxSharedArticlesSeedBatching(BatchedPickListGeneration):
    abstract = False

    def get_inited_batcher(self):
        articles: Articles = load_pickle(self.input()["instance"]["articles"].path)
        resources: Resources = load_pickle(self.input()["instance"]["resources"].path)
        layout: LayoutData = load_pickle(self.input()["instance"]["layout"].path)
        layout_network = layout.layout_network
        batcher = SeedBatching(
            pick_cart=resources.resources[0].pick_cart,
            articles=articles,
            seed_criterion=SeedCriteria.CLOSEST_TO_DEPOT,
            similarity_measure=SimilarityMeasure.SHARED_ARTICLES,
            distance_matrix=layout_network.distance_matrix,
            start_node=layout_network.closest_node_to_start
        )
        return batcher

