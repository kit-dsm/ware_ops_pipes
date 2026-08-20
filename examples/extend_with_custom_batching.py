from cls_luigi.inhabitation_task import RepoMeta

from ware_ops_algos.algorithms import PriorityBatching
from ware_ops_algos.domain_models import Articles, Resources

from ware_ops_pipes.pipelines.io_helpers import load_pickle
from ware_ops_pipes.pipelines.templates.template_1 import MultiOrderBatching


class LargestOrderFirstBatching(PriorityBatching):
    algo_name = "LargestOrderFirstBatching"

    def _sorted_orders(self):
        return sorted(
            self.order_list,
            key=lambda order: -sum(
                position.amount for position in order.pick_positions
            ),
        )


class LargestOrderFirstBatchingComponent(MultiOrderBatching):
    abstract = False

    def get_inited_batcher(self):
        articles: Articles = load_pickle(
            self.input()["instance"]["articles"].path
        )
        resources: Resources = load_pickle(
            self.input()["instance"]["resources"].path
        )
        return LargestOrderFirstBatching(
            pick_cart=resources.resources[0].pick_cart,
            articles=articles,
        )


def main() -> None:
    registered = any(
        task.cls is LargestOrderFirstBatchingComponent
        for task in RepoMeta.repository
    )
    if not registered:
        raise RuntimeError("The pipeline component was not registered.")

    print("Registered LargestOrderFirstBatchingComponent for synthesis")


if __name__ == "__main__":
    main()
