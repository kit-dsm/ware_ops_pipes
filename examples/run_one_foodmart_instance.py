import argparse
from pathlib import Path
from typing import Tuple

from ware_ops_algos.data_loaders import FoodmartLoader
from ware_ops_algos.domain_models import load_and_flatten_data_card

from ware_ops_pipes.synthesis.runner import PipelineRunner


ROOT = Path(__file__).resolve().parents[1]


class SingleInstanceRunner(PipelineRunner):
    def __init__(self, instance_path: Path, **kwargs):
        self.instance_path = instance_path
        super().__init__(
            instance_set_name="FoodmartExample",
            instances_dir=instance_path.parent,
            project_root=ROOT,
            **kwargs,
        )

    def discover_instances(self) -> list[Tuple[str, list[Path]]]:
        return [(self.instance_path.stem, [self.instance_path])]

    def _build_pipelines(self):
        return super()._build_pipelines()[:self.max_pipelines]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("instance", type=Path)
    parser.add_argument("--max-pipelines", type=int, default=3)
    args = parser.parse_args()

    instance_path = args.instance.resolve()
    if not instance_path.is_file():
        parser.error(f"instance file not found: {instance_path}")

    data_card = load_and_flatten_data_card(ROOT / "data" / "data_cards" / "foodmart.yaml")
    runner = SingleInstanceRunner(
        instance_path=instance_path,
        data_card=data_card,
        excluded=["ExactSolving", "CombinedBatchingRoutingAssigning"],
        max_pipelines=args.max_pipelines,
        time_limit_sec=240,
        loader_cls=FoodmartLoader,
        verbose=False,
    )
    runner.run_instance(instance_path.stem, [instance_path])
    runner.save_runtimes()


if __name__ == "__main__":
    main()
