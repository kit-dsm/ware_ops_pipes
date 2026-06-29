from ware_ops_algos.algorithms import EDDScheduling
from ware_ops_pipes.pipelines.templates.cosy_template import AbstractScheduling


class EDDScheduler(AbstractScheduling):
    abstract = False

    def _get_inited_scheduler(self):
        resources = self._load_resources()
        scheduler = EDDScheduling(resources)
        return scheduler



