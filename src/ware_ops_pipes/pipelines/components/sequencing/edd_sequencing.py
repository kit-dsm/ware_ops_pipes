from ware_ops_algos.algorithms import EDDSequencer, PickListSequencer
from ware_ops_pipes.pipelines.templates.template_1 import RoutingNeutralSequencing


class EDDSequencing(RoutingNeutralSequencing):
    abstract = False

    def _get_inited_sequencer(self) -> PickListSequencer:
        orders = self._load_orders()
        resources = self._load_resources()
        scheduler = EDDSequencer(orders=orders,
                                 resources=resources)
        return scheduler
