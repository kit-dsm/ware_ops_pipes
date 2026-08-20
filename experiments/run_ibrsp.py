import argparse
from pathlib import Path
from typing import Tuple

from ware_ops_algos.data_loaders import IBRSPLoader
from ware_ops_algos.domain_models import load_and_flatten_data_card
from ware_ops_pipes.synthesis.runner import PipelineRunner


class IBRSPRunner(PipelineRunner):
    """Runner for IBRSP instances"""

    def discover_instances(self) -> list[Tuple[str, list[Path]]]:
        instances = []
        for filepath in self.instances_dir.glob("*.txt"):
            if filepath.is_file():
                instances.append((filepath.stem, [filepath]))
        return instances


def main():
    print("Importing template and subproblems...")

    # Configuration
    parser = argparse.ArgumentParser()
    parser.add_argument("instance_set",
                        choices=["KrisSmallDataCorrected", "KrisLargeData"],
                        nargs="?",
                        default="KrisSmallDataCorrected")
    parser.add_argument("--workers", type=int, default=1,
                        help="Number of Luigi workers (parallel pipelines).")
    args = parser.parse_args()
    instance_set = args.instance_set

    PROJECT_ROOT = Path(__file__).parent.parent
    DATA_DIR = PROJECT_ROOT / "data"

    instances_base = DATA_DIR / "instances"
    dc = load_and_flatten_data_card(DATA_DIR / "data_cards" / "kris.yaml")
    runner = IBRSPRunner(instance_set, instances_base / instance_set, PROJECT_ROOT,
                         data_card=dc,
                         excluded=["ExactSolving",
                                   "CombinedBatchingRoutingAssigning"],
                         loader_cls=IBRSPLoader,
                         verbose=True,
                         time_limit_sec=240,
                         workers=args.workers)
    runner.run_all()
    print(runner.pipeline_runtimes)


if __name__ == "__main__":
    main()
