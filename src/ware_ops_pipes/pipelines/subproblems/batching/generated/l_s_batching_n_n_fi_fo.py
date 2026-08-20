# This file is generated. Do not edit manually.

from ware_ops_pipes.pipelines.resolve_algo_class_names import (
    resolve_algorithm_class,
)

from ..configured_batching import ConfiguredLocalSearchBatching
from ._support import get_algorithm_card


class LSBatchingNNFiFo(ConfiguredLocalSearchBatching):
    abstract = False

    algorithm_card = get_algorithm_card('LSBatchingNNFiFo')

    routing_class = resolve_algorithm_class('NearestNeighbourhoodRouting')
    start_batching_class = resolve_algorithm_class(
        'FifoBatching'
    )

    neighborhood_classes = [
        resolve_algorithm_class('SwapNeighborhood'),
        resolve_algorithm_class('ShiftNeighborhood'),
    ]
