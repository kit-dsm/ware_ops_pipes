from os import makedirs

from cls_luigi.inhabitation_task import LuigiCombinator
from os.path import join as pjoin
from luigi import LocalTarget, Task
from ware_ops_pipes.pipelines.pipeline_params import PipelineParams


class BaseComponent(Task, LuigiCombinator):
    abstract = True

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.pipeline_params = PipelineParams()
        makedirs(self.pipeline_params.output_folder, exist_ok=True)

    def get_luigi_local_target_with_task_id(
            self,
            out_name
    ) -> LocalTarget:
        return LocalTarget(
            pjoin(self.pipeline_params.output_folder,
                  self.task_id + "_" + out_name)
        )

