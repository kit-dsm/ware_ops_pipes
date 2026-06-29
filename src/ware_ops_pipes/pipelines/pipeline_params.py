import copy
from dataclasses import dataclass, field
from os import getcwd
from os.path import join as pjoin


@dataclass
class PipelineParams:
    output_folder: str = field(default_factory=lambda: pjoin(getcwd(), "outputs"))
    seed: int = 42
    time_limit_sec: int | None = None
    gen_tour: bool = False
    instance_set_name: str | None = None
    instance_name: str | None = None
    instance_path: str | None = None
    domain_path: str | None = None


_PARAMS = PipelineParams()


def get_pipeline_params() -> PipelineParams:
    return _PARAMS


def set_pipeline_params(
    instance_set_name, instance_name, instance_path,
    output_folder, domain_path,
    time_limit_seconds=None, gen_tour=False,
) -> None:
    _PARAMS.instance_set_name = instance_set_name
    _PARAMS.instance_name = instance_name
    _PARAMS.instance_path = instance_path
    _PARAMS.output_folder = output_folder
    _PARAMS.domain_path = domain_path
    _PARAMS.time_limit_sec = time_limit_seconds
    _PARAMS.gen_tour = gen_tour