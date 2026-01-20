from ware_ops_algos.algorithms import LPTScheduling
from ware_ops_pipes.pipelines.templates.template_1 import AbstractScheduling


class LPTScheduler(AbstractScheduling):
    abstract = False

    def _get_inited_sequencer(self):
        scheduler = LPTScheduling()
        return scheduler



