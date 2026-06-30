from __future__ import annotations

import json
import time
from abc import abstractmethod, ABC
from typing import Tuple
from pathlib import Path

import luigi
from cosy.maestro import Maestro
from cosy_luigi import CoSyLuigiRepo
from luigi.task_register import Register

from ware_ops_algos.data_loaders import DataLoader
from ware_ops_algos.domain_algo_mapper.domain_algo_mapper import DomainAlgorithmMapper
from ware_ops_algos.domain_models.base_domain import BaseWarehouseDomain
from ware_ops_algos.taxonomy.taxonomy import TAXONOMY
from ware_ops_algos.algorithms.algorithm_cards import import_algo_class, load_packaged_algo_cards

from ware_ops_pipes import print_tree
from ware_ops_pipes.pipelines.pipeline_params import set_pipeline_params
from ware_ops_pipes.pipelines.subproblems.batching.clark_and_wright_nn import ClarkAndWrightNN
from ware_ops_pipes.pipelines.subproblems.batching.clark_and_wright_rr import ClarkAndWrightRR
from ware_ops_pipes.pipelines.subproblems.batching.clark_and_wright_sshape import ClarkAndWrightSShape
from ware_ops_pipes.pipelines.subproblems.batching.due_date import DueDate
from ware_ops_pipes.pipelines.subproblems.batching.fifo import FiFo
from ware_ops_pipes.pipelines.subproblems.batching.ls_nn_due import LSBatchingNNDueDate
from ware_ops_pipes.pipelines.subproblems.batching.ls_nn_fifo import LSBatchingNNFiFo
from ware_ops_pipes.pipelines.subproblems.batching.ls_nn_fifo_ord_nr import LSBatchingNNFiFoOrderNr
from ware_ops_pipes.pipelines.subproblems.batching.ls_nn_rand import LSBatchingNNRand
from ware_ops_pipes.pipelines.subproblems.batching.ls_rr import LSBatchingRR
from ware_ops_pipes.pipelines.subproblems.batching.order_nr_fifo import OrderNrFiFo
from ware_ops_pipes.pipelines.subproblems.batching.random import Random
from ware_ops_pipes.pipelines.subproblems.batching.seed import ClosestDepotMinDistanceSeedBatching
from ware_ops_pipes.pipelines.subproblems.batching.seed_shared_articles import ClosestDepotMaxSharedArticlesSeedBatching
from ware_ops_pipes.pipelines.subproblems.item_assignment.greedy_item_assignment import GreedyIA
from ware_ops_pipes.pipelines.subproblems.item_assignment.min_max_item_assignment import MinMaxIA
from ware_ops_pipes.pipelines.subproblems.item_assignment.min_min_item_assignment import MinMinIA
from ware_ops_pipes.pipelines.subproblems.item_assignment.nn_item_assignment import NNIA
from ware_ops_pipes.pipelines.subproblems.item_assignment.single_pos_item_assignment import SinglePosIA
from ware_ops_pipes.pipelines.subproblems.routing.joint_batching_routing_assigning import \
    CombinedBatchingRoutingAssigning
from ware_ops_pipes.pipelines.subproblems.routing.largest_gap import LargestGap
from ware_ops_pipes.pipelines.subproblems.routing.midpoint import Midpoint
from ware_ops_pipes.pipelines.subproblems.routing.nn import NearestNeighbourhood
from ware_ops_pipes.pipelines.subproblems.routing.return_algo import Return
from ware_ops_pipes.pipelines.subproblems.routing.s_shape import SShape
from ware_ops_pipes.pipelines.subproblems.routing.sprp import RatliffRosenthal
from ware_ops_pipes.pipelines.subproblems.scheduling.edd_scheduling import EDDScheduler
from ware_ops_pipes.pipelines.subproblems.scheduling.lpt_scheduling import LPTScheduler
from ware_ops_pipes.pipelines.subproblems.scheduling.spt_scheduling import SPTScheduler
from ware_ops_pipes.ranking.ranking import RankingEvaluator

class PipelineRunner(ABC):
    """Base class for running pipelines on warehouse instances"""

    def __init__(
            self,
            instance_set_name: str,
            instances_dir: Path,
            cache_dir: Path,
            project_root: Path,
            data_card,
            excluded: list = [],
            max_pipelines: int = None,
            verbose: bool = True,
            cleanup: bool = True,
            ranker=RankingEvaluator,
            time_limit_sec: int | None = None,
            gen_tour: bool = False
    ):
        self.instance_set_name = instance_set_name
        self.instances_dir = Path(instances_dir)
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.cache_path: Path | None = None

        self.project_root = Path(project_root)
        self.src_dir = project_root / "src" / "warehouse_algos"
        self.max_pipelines = max_pipelines
        self.verbose = verbose
        self.cleanup = cleanup
        self.pipeline_runtimes = {}
        self.time_limit_sec = time_limit_sec
        self.gen_tour = gen_tour

        self.repo_class_by_algo_name = {
            "GreedyIA": GreedyIA,
            "NNIA": NNIA,
            "SinglePosIA": SinglePosIA,
            "MinMinIA": MinMinIA,
            "MinMaxIA": MinMaxIA,

            "FiFo": FiFo,
            "OrderNrFiFo": OrderNrFiFo,
            "DueDate": DueDate,
            "Random": Random,
            "ClarkAndWrightNN": ClarkAndWrightNN,
            "ClarkAndWrightRR": ClarkAndWrightRR,
            "ClarkAndWrightSShape": ClarkAndWrightSShape,
            "LSBatchingNNDueDate": LSBatchingNNDueDate,
            "LSBatchingRR": LSBatchingRR,
            "LSBatchingNNRand": LSBatchingNNRand,
            "LSBatchingNNFiFo": LSBatchingNNFiFo,
            "LSBatchingNNFiFoOrderNr": LSBatchingNNFiFoOrderNr,
            "ClosestDepotMinDistanceSeedBatching": ClosestDepotMinDistanceSeedBatching,
            "ClosestDepotMaxSharedArticlesSeedBatching": ClosestDepotMaxSharedArticlesSeedBatching,

            "SShape": SShape,
            "LargestGap": LargestGap,
            "Midpoint": Midpoint,
            "Return": Return,
            "NearestNeighbourhood": NearestNeighbourhood,
            "RatliffRosenthal": RatliffRosenthal,
            "CombinedBatchingRoutingAssigning": CombinedBatchingRoutingAssigning,

            # Card name -> CoSy-Luigi component class.
            "EDDScheduling": EDDScheduler,
            "LPTScheduling": LPTScheduler,
            "SPTScheduling": SPTScheduler,
        }

        self.algos = load_packaged_algo_cards()
        if self.verbose:
            print(f"Loaded {len(self.algos)} model cards")
        self.data_card = data_card
        self.excluded = excluded
        self.loader: DataLoader | None = None
        self.ranker = ranker

    @abstractmethod
    def discover_instances(self) -> list[Tuple[str, list[Path]]]:
        """
        Discover instances in the directory.
        """
        pass

    @abstractmethod
    def load_domain(self, instance_name: str, file_paths: list[Path]) -> BaseWarehouseDomain:
        """
        Load domain for an instance.
        """
        pass

    def run_all(self):
        """
        Run pipelines for all discovered instances
        """
        instances = self.discover_instances()

        print(f"\n{'=' * 80}")
        print(f"Instance Set: {self.instance_set_name}")
        print(f"Found {len(instances)} instances")
        print(f"{'=' * 80}\n")
        for instance_name, file_paths in instances:
            try:
                self.run_instance(instance_name, file_paths)
            except Exception as e:
                print(f"❌ Error processing {instance_name}: {e}")
                if self.verbose:
                    import traceback
                    traceback.print_exc()
        self.save_runtimes()

    def run_instance(self, instance_name: str, file_paths: list[Path]):
        """Run pipelines for a single instance"""

        print(f"\n{'=' * 80}")
        print(f"Processing: {instance_name}")
        print(f"{'=' * 80}\n")
        Register.clear_instance_cache()
        timings = {}
        # Load domain (with caching)
        t0 = time.perf_counter()
        domain = self.load_domain(instance_name, file_paths)
        timings["load_domain"] = time.perf_counter() - t0

        # Setup output folder
        output_folder = (
                self.project_root / "experiments" / "output"
                / self.instance_set_name / instance_name
        )
        output_folder.mkdir(parents=True, exist_ok=True)

        # Get cache path for domain
        # cache_path = self.cache_dir / f"{instance_name}_domain.pkl"

        # Set pipeline parameters
        set_pipeline_params(
            output_folder=str(output_folder),
            instance_set_name=self.instance_set_name,
            instance_name=instance_name,
            instance_path=str(file_paths[0]),
            domain_path=str(self.loader.cache_path),
            time_limit_seconds=self.time_limit_sec,
            gen_tour=self.gen_tour
        )

        t0 = time.perf_counter()
        final_algos = self._filter_applicable_algorithms()
        timings["filter_algorithms"] = time.perf_counter() - t0

        t0 = time.perf_counter()
        pipelines = self._build_pipelines(final_algos)
        timings["build_pipelines"] = time.perf_counter() - t0

        t0 = time.perf_counter()
        if pipelines:
            print(f"\n✓ Running {len(pipelines)} pipelines...\n")
            luigi.interface.InterfaceLogging.setup(type('opts',
                                                        (),
                                                        {'background': None,
                                                         'logdir': None,
                                                         'logging_conf_file': None,
                                                         'log_level': 'DEBUG'
                                                         }))
            luigi.build(pipelines, local_scheduler=True)

            self.create_ranking(instance_name, output_folder)
            # if self.cleanup:
            #     self._cleanup(output_folder)
            timings["run_pipelines"] = time.perf_counter() - t0
            timings["total"] = sum(timings.values())
            self.pipeline_runtimes[instance_name] = timings
        else:
            print("⚠ No valid pipelines found!")

    def _import_models(self, algos_applicable):
        """Import applicable model implementations"""
        for algo in algos_applicable:
            algo_name = algo.algo_name
            if algo_name not in self.implementation_module:
                if self.verbose:
                    print(f"⚠ Unknown model: {algo_name}, skipping...")
                continue

            try:
                module_path = self.implementation_module[algo_name]
                cls = import_algo_class(algo_name, module_path)
                if self.verbose:
                    print(f"✅ {algo_name}")
            except Exception as e:
                if self.verbose:
                    print(f"❌ Failed to import {algo_name}: {e}")

    def _filter_applicable_algorithms(self):
        """Return algorithm cards applicable to the current data card and present in the CoSy repo."""

        mapper = DomainAlgorithmMapper(TAXONOMY)

        applicable = mapper.filter(
            algorithms=self.algos,
            instance=self.data_card,
            verbose=self.verbose,
        )

        final_algos = []

        for algo in applicable:
            if algo.algo_name in self.excluded:
                if self.verbose:
                    print(f"⚠ Excluded by config: {algo.algo_name}")
                continue

            if algo.algo_name not in self.repo_class_by_algo_name:
                if self.verbose:
                    print(f"⚠ No CoSy component registered for applicable algorithm: {algo.algo_name}")
                continue

            final_algos.append(algo)

        if self.verbose:
            print(
                f"✓ {len(final_algos)}/{len(self.algos)} algorithms usable "
                f"after domain filtering and exclusions"
            )

        return final_algos

    def _build_pipelines(self, final_algos):
        """Build valid pipelines using CoSy over the data-card-filtered repo."""

        from itertools import islice

        from ware_ops_pipes.pipelines.templates.cosy_template import (
            InstanceLoader,
            ResultAggregationDistance,
        )

        model_classes = []
        seen = set()

        for algo in final_algos:
            cls = self.repo_class_by_algo_name[algo.algo_name]

            if cls in seen:
                continue

            model_classes.append(cls)
            seen.add(cls)

        repo_classes = [
            InstanceLoader,
            *model_classes,
            ResultAggregationDistance,
        ]

        if self.verbose:
            print("\nCoSy repository classes:")
            for cls in repo_classes:
                print(f"  - {cls.__name__}")

        endpoint = ResultAggregationDistance

        # endpoint.configure(self.data_card, final_algos)

        repo = CoSyLuigiRepo(*repo_classes)
        maestro = Maestro(repo.cls_repo, repo.taxonomy)

        query = maestro.query(endpoint.target())

        if self.max_pipelines is None or self.max_pipelines <= 0:
            pipelines = list(query)
        else:
            pipelines = list(islice(query, self.max_pipelines))

        if self.verbose and pipelines:
            print(f"✓ Found {len(pipelines)} valid pipelines")
            for i, pipeline in enumerate(pipelines[:3], 1):
                print(f"\nPipeline {i}:")
                print(print_tree(pipeline))

        return pipelines

    def _cleanup(self, output_folder: Path):
        """Clean up intermediate files"""
        try:
            for file_path in output_folder.glob("InstanceLoader__*.pkl"):
                file_path.unlink()
                if self.verbose:
                    print(f"Deleted {file_path.name}")
        except Exception as e:
            print(f"Cleanup failed: {e}")

    def create_ranking(self, instance_name: str, output_folder: Path):
        """Create ranking for this instance"""
        try:
            ranker = self.ranker(
                output_dir=str(output_folder),
                instance_name=instance_name,
                taxonomy=TAXONOMY,
                data_card=self.data_card
            )
            df = ranker.evaluate()

            # Best pipeline is first row
            if not df.empty:
                best = df.iloc[0]
                print(f"Best: {best['pipeline_id']} = {best['value']:.2f}")
                return best

        except Exception as e:
            print(f"⚠ Ranking error: {e}")

    def save_runtimes(self):
        output_folder = (
                self.project_root / "experiments" / "output"
                / "runtimes"
        )
        output_folder.mkdir(parents=True, exist_ok=True)
        with open(output_folder / f"{self.instance_set_name}.json", "w") as f:
            json.dump(self.pipeline_runtimes, f, indent=2)
