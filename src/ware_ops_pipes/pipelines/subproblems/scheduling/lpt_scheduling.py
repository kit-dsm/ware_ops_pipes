from ware_ops_algos.algorithms import LPTScheduling
from ware_ops_pipes.pipelines.templates.cosy_template import AbstractScheduling


class LPTScheduler(AbstractScheduling):
    abstract = False
    algo_cls = LPTScheduling

    def _get_inited_scheduler(self):
        resources = self._load_resources()
        scheduler = LPTScheduling(resources)
        return scheduler



