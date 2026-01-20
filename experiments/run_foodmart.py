import time
from pathlib import Path
from typing import Tuple

from ware_ops_algos.data_loaders import FoodmartLoader
from ware_ops_algos.domain_models import BaseWarehouseDomain
from ware_ops_pipes.utils.experiment_utils import PipelineRunner


class FoodmartRunner(PipelineRunner):
    """Runner for Foodmart instances"""

    def __init__(self, instance_set_name: str, instances_dir: Path, cache_dir: Path,
                 project_root: Path, **kwargs):
        super().__init__(instance_set_name, instances_dir, cache_dir, project_root, **kwargs)
        self.loader = FoodmartLoader(str(instances_dir), str(cache_dir))

    def discover_instances(self) -> list[Tuple[str, list[Path]]]:
        instances = []
        for filepath in self.instances_dir.glob("*.txt"):
            if filepath.is_file():
                instances.append((filepath.stem, [filepath]))
        return instances

    def load_domain(self, instance_name: str, file_paths: list[Path]) -> BaseWarehouseDomain:
        return self.loader.load(file_paths[0].name, use_cache=True)


def main():
    print("Importing template and components...")

    # Configuration
    PROJECT_ROOT = Path(__file__).parent.parent
    DATA_DIR = PROJECT_ROOT / "data"

    instances_base = DATA_DIR / "instances"
    cache_base = DATA_DIR / "instances" / "caches"
    runner = FoodmartRunner("FoodmartData", instances_base / "FoodmartData",
                            cache_base / "FoodmartData", PROJECT_ROOT, verbose=True)
    runner.run_all()
    print(runner.pipeline_runtimes)


if __name__ == "__main__":
    main()
