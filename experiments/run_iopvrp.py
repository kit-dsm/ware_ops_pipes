import re

from pathlib import Path
from typing import Tuple

from ware_ops_algos.data_loaders import FoodmartLoader, IOPVRPLoader
from ware_ops_algos.domain_models import BaseWarehouseDomain
from ware_ops_pipes.utils.experiment_utils import PipelineRunner


class IOPVRPRunner(PipelineRunner):
    """Runner for IOPVRP instances (paired files)"""

    PAIR_RE = re.compile(
        r'^(?P<kind>OrderList|OrderLineList)_(?P<set>[^_]+)_(?P<inst>\d+)_(?P<rep>\d+)\.[A-Za-z0-9]+$'
    )

    def __init__(self, instance_set_name: str, instances_dir: Path, cache_dir: Path,
                 project_root: Path, **loader_kwargs):
        runner_kwargs = {k: v for k, v in loader_kwargs.items()
                         if k in ['max_pipelines', 'verbose', 'cleanup']}
        loader_only_kwargs = {k: v for k, v in loader_kwargs.items()
                              if k not in runner_kwargs}

        super().__init__(instance_set_name, instances_dir, cache_dir, project_root, **runner_kwargs)
        self.loader = IOPVRPLoader(instances_dir, cache_dir, **loader_only_kwargs)

    def discover_instances(self) -> list[Tuple[str, list[Path]]]:
        by_key = {}
        for p in self.instances_dir.iterdir():
            if not p.is_file():
                continue
            m = self.PAIR_RE.match(p.name)
            if not m:
                continue

            kind = m['kind']
            key = (m['set'], int(m['inst']), int(m['rep']))
            entry = by_key.setdefault(key, {})
            entry[kind] = p

        instances = []
        for (iset, inst, rep), files_dict in by_key.items():
            if 'OrderList' in files_dict and 'OrderLineList' in files_dict:
                instance_name = f"{iset}_{inst}_{rep}"
                instances.append((
                    instance_name,
                    [files_dict['OrderList'], files_dict['OrderLineList']]
                ))

        return instances

    def load_domain(self, instance_name: str, file_paths: list[Path]) -> BaseWarehouseDomain:
        return self.loader.load(
            file_paths[0].name,
            file_paths[1].name,
            use_cache=True
        )


def main():
    print("Importing template and components...")

    # Configuration
    PROJECT_ROOT = Path(__file__).parent.parent
    DATA_DIR = PROJECT_ROOT / "data"

    instances_base = DATA_DIR / "instances"
    cache_base = DATA_DIR / "instances" / "caches"

    runner = IOPVRPRunner("IOPVRP", instances_base / "IOPVRP",
                            cache_base / "IOPVRP", PROJECT_ROOT, verbose=True)

    runner.run_all()


if __name__ == "__main__":
    main()
