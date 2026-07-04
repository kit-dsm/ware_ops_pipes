from ware_ops_algos.algorithms import SPTScheduling
from ware_ops_pipes.pipelines.templates.template_1 import AbstractScheduling


class SPTScheduler(AbstractScheduling):
    abstract = False

    def _get_inited_scheduler(self):
        resources = self._load_resources()
        scheduler = SPTScheduling(resources)
        return scheduler



