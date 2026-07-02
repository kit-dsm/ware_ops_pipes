from __future__ import annotations

from ware_ops_algos.algorithms.batching.batching import LocalSearchBatching
from ware_ops_algos.domain_models import Articles, LayoutData, Resources

from ware_ops_pipes.pipelines.io_helpers import load_pickle
from ware_ops_pipes.pipelines.resolve_algo_class_names import resolve_algorithm_class
from ware_ops_pipes.pipelines.templates.cosy_template import (
    MultiOrderBatching,
    class_fingerprint_payload,
)


_GENERATED_LS_CLASSES = {}


class ConfiguredLocalSearchBatching(MultiOrderBatching):
    abstract = True

    algo_cls = LocalSearchBatching
    algorithm_card = None

    # These are intentionally class attributes.
    # CoSy constraints must be able to inspect them before Luigi execution.
    routing_class = None
    start_batching_cls = None

    def implementation_config(self) -> dict:
        return self.algorithm_card.implementation or {}

    def config_fingerprint_payload(self) -> dict:
        return {
            "algo_name": self.algorithm_card.algo_name,
            "implementation": self.implementation_config(),
            "routing_class": class_fingerprint_payload(self.routing_class),
            "start_batching_cls": class_fingerprint_payload(self.start_batching_cls),
            "runtime": {
                "time_limit_sec": self.pipeline_params.time_limit_sec,
                "gen_tour": self.pipeline_params.gen_tour,
            },
        }

    def get_inited_batcher(self):
        articles: Articles = load_pickle(self.input()["instance"]["articles"].path)
        resources: Resources = load_pickle(self.input()["instance"]["resources"].path)
        layout: LayoutData = load_pickle(self.input()["instance"]["layout"].path)

        layout_network = layout.layout_network
        graph_nodes = list(layout_network.graph.nodes)

        routing_kwargs = {
            "start_node": layout_network.start_node,
            "end_node": layout_network.end_node,
            "closest_node_to_start": layout_network.closest_node_to_start,
            "min_aisle_position": layout_network.min_aisle_position,
            "max_aisle_position": layout_network.max_aisle_position,
            "distance_matrix": layout_network.distance_matrix,
            "predecessor_matrix": layout_network.predecessor_matrix,
            "picker": resources.resources,
            "gen_tour": self.pipeline_params.gen_tour,
            "gen_item_sequence": self.pipeline_params.gen_tour,
            "node_list": layout_network.node_list,
            "node_to_idx": {node: idx for idx, node in enumerate(graph_nodes)},
            "idx_to_node": {idx: node for idx, node in enumerate(graph_nodes)},
        }

        return LocalSearchBatching(
            pick_cart=resources.resources[0].pick_cart,
            articles=articles,
            routing_class=self.routing_class,
            routing_class_kwargs=routing_kwargs,
            start_batching_class=self.start_batching_cls,
            time_limit=self.pipeline_params.time_limit_sec,
        )


def make_configured_local_search_component(card):
    impl = card.implementation or {}

    component_name = impl["component_name"]

    if component_name in _GENERATED_LS_CLASSES:
        return _GENERATED_LS_CLASSES[component_name]

    routing_cls = resolve_algorithm_class(impl["routing_class"])
    start_batching_cls = resolve_algorithm_class(impl["start_batching_class"])

    cls = type(
        component_name,
        (ConfiguredLocalSearchBatching,),
        {
            "abstract": False,
            "algorithm_card": card,
            "routing_class": routing_cls,
            "start_batching_cls": start_batching_cls,
            "__module__": __name__,
            "__qualname__": component_name,
        },
    )

    _GENERATED_LS_CLASSES[component_name] = cls
    return cls