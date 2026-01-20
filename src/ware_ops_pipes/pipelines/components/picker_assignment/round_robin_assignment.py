from ware_ops_algos.algorithms import RoundRobinAssigner, Assigner
from ware_ops_pipes.pipelines.templates.template_1 import AbstractPickerAssignment


class RRAssigner(AbstractPickerAssignment):
    abstract = False

    def _get_inited_assigner(self) -> Assigner:
        resources_domain = self._load_resources()
        assigner = RoundRobinAssigner(resources=resources_domain.resources)
        return assigner
