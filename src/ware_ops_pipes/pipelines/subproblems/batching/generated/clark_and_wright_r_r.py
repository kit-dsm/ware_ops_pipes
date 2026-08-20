# This file is generated. Do not edit manually.

from ware_ops_pipes.pipelines.resolve_algo_class_names import (
    resolve_algorithm_class,
)

from ..configured_batching import ConfiguredClarkAndWrightBatching
from ._support import get_algorithm_card


class ClarkAndWrightRR(ConfiguredClarkAndWrightBatching):
    abstract = False

    algorithm_card = get_algorithm_card('ClarkAndWrightRR')

    routing_class = resolve_algorithm_class('RatliffRosenthalRouting')
