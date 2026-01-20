from ware_ops_pipes.pipelines.components.routing_config.exact_config import ExactConfig
from ware_ops_pipes.utils.io_helpers import dump_json


class ExactConfig1(ExactConfig):
    abstract = False

    def run(self):
        config = self._get_config()
        dump_json(self.output()["routing_config"].path, config)

    def _get_config(self):
        return {
            "big_m": 1000,
            "objective": "minimize_distance", # TODO needs to come from instance
            "set_time_limit": 60
        }
