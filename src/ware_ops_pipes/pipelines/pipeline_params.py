import copy
from dataclasses import dataclass, field
from os import getcwd
from os.path import join as pjoin
from typing import Any


@dataclass
class PipelineParams:
    output_folder: str = field(default_factory=lambda: pjoin(getcwd(), "outputs"))
    data_cache_folder: str | None = None

    algorithm_config_path: str | None = None
    seed: int = 42
    time_limit_sec: int | None = None
    gen_tour: bool = False

    instance_set_name: str | None = None
    instance_name: str | None = None
    instance_path: str | None = None
    instances_dir: str | None = None

    loader_cls: type | None = None
    loader_kwargs: dict[str, Any] = field(default_factory=dict)


_PARAMS = PipelineParams()


def get_pipeline_params() -> PipelineParams:
    return _PARAMS


def set_pipeline_params(
    instance_set_name: str,
    instance_name: str,
    instance_path: str,
    instances_dir: str,
    output_folder: str,
    data_cache_folder: str,
    loader_cls: type,
    loader_kwargs: dict | None = None,
    time_limit_seconds: int | None = None,
    gen_tour: bool = False,
    algorithm_config_path: str | None = None,
) -> None:
    _PARAMS.instance_set_name = instance_set_name
    _PARAMS.instance_name = instance_name
    _PARAMS.instance_path = instance_path
    _PARAMS.instances_dir = instances_dir

    _PARAMS.output_folder = output_folder
    _PARAMS.data_cache_folder = data_cache_folder

    _PARAMS.loader_cls = loader_cls
    _PARAMS.loader_kwargs = dict(loader_kwargs or {})

    _PARAMS.time_limit_sec = time_limit_seconds
    _PARAMS.gen_tour = gen_tour
    _PARAMS.algorithm_config_path = algorithm_config_path