from __future__ import annotations

from typing import ClassVar

from ware_ops_algos.algorithms import (
    LocalSearchBatchingModular,
    SeedBatchingModular,
    ClosestToDepotSeed,
    MinDistanceSimilarity, RatliffRosenthalRouting,
)
from ware_ops_algos.domain_models import Articles, LayoutData, Resources
from ware_ops_pipes.pipelines.io_helpers import load_pickle
from ware_ops_pipes.pipelines.templates.template_1 import MultiOrderBatching


class ConfiguredSeedBatching(MultiOrderBatching):
    """Abstract CLS-Luigi wrapper for configured SeedBatching variants."""

    abstract = True

    algorithm_card = None

    seed_criterion_cls: ClassVar[type | None] = None
    similarity_measure_cls: ClassVar[type | None] = None

    def get_inited_batcher(self) -> SeedBatchingModular:
        articles: Articles = load_pickle(
            self.input()["instance"]["articles"].path
        )
        resources: Resources = load_pickle(
            self.input()["instance"]["resources"].path
        )
        layout: LayoutData = load_pickle(
            self.input()["instance"]["layout"].path
        )

        network = layout.layout_network

        seed_criterion = self._create_seed_criterion(network)
        similarity_measure = self._create_similarity_measure(network)

        return SeedBatchingModular(
            pick_cart=resources.resources[0].pick_cart,
            articles=articles,
            seed_criterion=seed_criterion,
            similarity_measure=similarity_measure,
        )

    def _create_seed_criterion(self, network):
        if self.seed_criterion_cls is None:
            raise RuntimeError(
                f"{type(self).__name__} does not define seed_criterion_cls."
            )

        if self.seed_criterion_cls is ClosestToDepotSeed:
            return self.seed_criterion_cls(
                distance_matrix=network.distance_matrix,
                start_node=network.closest_node_to_start,
            )

        return self.seed_criterion_cls()

    def _create_similarity_measure(self, network):
        if self.similarity_measure_cls is None:
            raise RuntimeError(
                f"{type(self).__name__} does not define similarity_measure_cls."
            )

        if self.similarity_measure_cls is MinDistanceSimilarity:
            return self.similarity_measure_cls(
                distance_matrix=network.distance_matrix,
            )

        return self.similarity_measure_cls()


class ConfiguredLocalSearchBatching(MultiOrderBatching):
    abstract = True

    algorithm_card = None
    routing_class = None
    start_batching_class = None
    neighborhood_classes = None

    def get_inited_batcher(self):
        articles: Articles = load_pickle(
            self.input()["instance"]["articles"].path
        )
        resources: Resources = load_pickle(
            self.input()["instance"]["resources"].path
        )
        layout: LayoutData = load_pickle(
            self.input()["instance"]["layout"].path
        )

        if self.routing_class is None:
            raise RuntimeError(
                f"{type(self).__name__} does not define routing_class."
            )

        if self.start_batching_class is None:
            raise RuntimeError(
                f"{type(self).__name__} does not define "
                "start_batching_class."
            )

        if not self.neighborhood_classes:
            raise RuntimeError(
                f"{type(self).__name__} does not define "
                "neighborhood_classes."
            )

        routing_kwargs = self._create_routing_kwargs(
            layout=layout,
            resources=resources,
        )

        return LocalSearchBatchingModular(
            pick_cart=resources.resources[0].pick_cart,
            articles=articles,
            routing_class=self.routing_class,
            routing_class_kwargs=routing_kwargs,
            start_batching_class=self.start_batching_class,
            neighborhood_classes=self.neighborhood_classes,
            time_limit=self.pipeline_params.time_limit_sec,
            verbose=False,
        )

    def _create_routing_kwargs(
            self,
            layout: LayoutData,
            resources: Resources,
    ) -> dict:
        network = layout.layout_network
        graph_nodes = list(network.graph.nodes)

        kwargs = {
            "start_node": network.start_node,
            "end_node": network.end_node,
            "closest_node_to_start": network.closest_node_to_start,
            "min_aisle_position": network.min_aisle_position,
            "max_aisle_position": network.max_aisle_position,
            "distance_matrix": network.distance_matrix,
            "predecessor_matrix": network.predecessor_matrix,
            "picker": resources.resources,
            "gen_tour": self.pipeline_params.gen_tour,
            "gen_item_sequence": self.pipeline_params.gen_tour,
            "node_list": network.node_list,
            "node_to_idx": {
                node: idx
                for idx, node in enumerate(graph_nodes)
            },
            "idx_to_node": {
                idx: node
                for idx, node in enumerate(graph_nodes)
            },
        }

        if self.routing_class is RatliffRosenthalRouting:
            graph_params = layout.graph_data

            kwargs.update(
                {
                    "n_aisles": graph_params.n_aisles,
                    "n_pick_locations": graph_params.n_pick_locations,
                    "dist_aisle": graph_params.dist_aisle,
                    "dist_pick_locations": (
                        graph_params.dist_pick_locations
                    ),
                    "dist_aisle_location": (
                        graph_params.dist_bottom_to_pick_location
                    ),
                    "dist_start": graph_params.dist_start,
                    "dist_end": graph_params.dist_end,
                    "gen_tour": False,
                    "gen_item_sequence": False,
                }
            )

        return kwargs