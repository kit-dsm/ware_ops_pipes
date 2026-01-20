from ware_ops_algos.algorithms import EDDScheduling
from ware_ops_pipes.pipelines.templates.template_1 import AbstractScheduling


class EDDScheduler(AbstractScheduling):
    abstract = False

    def _get_inited_sequencer(self):
        scheduler = EDDScheduling()
        return scheduler



