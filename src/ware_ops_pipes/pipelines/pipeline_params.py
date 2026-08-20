import json
from os import getcwd
from os.path import join as pjoin
import luigi

def loader_name_from_cls(loader_cls: type) -> str:
    name = loader_cls.__name__

    if name == "FoodmartLoader":
        return "foodmart"

    if name == "HesslerIrnichLoader":
        return "hessler"

    if name == "IBRSPLoader":
        return "ibrsp"

    raise ValueError(f"Unknown loader class: {loader_cls}")


def get_loader_cls(loader_name: str):
    if loader_name == "foodmart":
        from ware_ops_pipes.data_loaders import FoodmartLoader
        return FoodmartLoader

    if loader_name == "hessler":
        from ware_ops_pipes.data_loaders import HesslerIrnichLoader
        return HesslerIrnichLoader

    if loader_name == "ibrsp":
        from ware_ops_pipes.data_loaders import IBRSPLoader
        return IBRSPLoader

    raise ValueError(f"Unknown loader_name: {loader_name!r}")

class PipelineParams(luigi.Config):
    output_folder = luigi.Parameter(default=pjoin(getcwd(), "outputs"))
    data_cache_folder = luigi.Parameter(
        default=pjoin(getcwd(), "outputs", "_data_cache")
    )

    seed = luigi.IntParameter(default=42)
    time_limit_sec = luigi.OptionalIntParameter(default=None)
    gen_tour = luigi.BoolParameter(default=False)

    instance_set_name = luigi.Parameter(default="")
    instance_name = luigi.Parameter(default="")
    instance_path = luigi.Parameter(default="")
    instances_dir = luigi.Parameter(default="")
    loader_name = luigi.Parameter(default="")
    loader_kwargs_json = luigi.Parameter(default="{}")

    def loader_kwargs(self) -> dict:
        return json.loads(self.loader_kwargs_json or "{}")

