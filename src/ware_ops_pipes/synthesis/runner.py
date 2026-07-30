from __future__ import annotations

import json
import time
from abc import abstractmethod, ABC
from typing import Tuple
from pathlib import Path

import luigi
from cls.fcl import FiniteCombinatoryLogic
from cls.subtypes import Subtypes
from cls_luigi.inhabitation_task import RepoMeta
from cls_luigi.unique_task_pipeline_validator import UniqueTaskPipelineValidator
from gurobipy._core import defaultdict

from ware_ops_algos.data_loaders import DataLoader
from ware_ops_algos.domain_models.base_domain import BaseWarehouseDomain
from ware_ops_algos.taxonomy.taxonomy import TAXONOMY
from ware_ops_algos.domain_algo_mapper.domain_algo_mapper import DomainAlgorithmMapper
from ware_ops_algos.algorithms.algorithm_cards import import_algo_class, load_packaged_algo_cards
from ware_ops_pipes.pipelines.subproblems.batching.generated.index import (
    CONFIGURED_COMPONENT_MODULES,
)

from ware_ops_pipes.ranking.ranking import RankingEvaluator
from ware_ops_pipes.pipelines import set_pipeline_params, inhabit, print_tree

class PipelineRunner(ABC):
    """Base class for running pipelines on warehouse instances"""

    def __init__(
            self,
            instance_set_name: str,
            instances_dir: Path,
            project_root: Path,
            data_card,
            excluded: list = [],
            max_pipelines: int = 10,
            verbose: bool = True,
            ranker=RankingEvaluator,
            time_limit_sec: int | None = None,
            gen_tour: bool = False,
            loader_kwargs: dict = defaultdict(),
            loader_cls=None
    ):
        self.instance_set_name = instance_set_name
        self.instances_dir = Path(instances_dir)
        self.loader_kwargs = loader_kwargs
        self.loader_cls = loader_cls

        self.project_root = Path(project_root)
        self.src_dir = project_root / "src" / "warehouse_algos"
        self.max_pipelines = max_pipelines
        self.verbose = verbose
        self.pipeline_runtimes = {}
        self.time_limit_sec = time_limit_sec
        self.gen_tour = gen_tour

        # Component implementations
        self.implementation_module = {
            "GreedyIA": "ware_ops_pipes.pipelines.subproblems.item_assignment.greedy_item_assignment",
            "NNIA": "ware_ops_pipes.pipelines.subproblems.item_assignment.nn_item_assignment",
            "SinglePosIA": "ware_ops_pipes.pipelines.subproblems.item_assignment.single_pos_item_assignment",
            "MinMinIA": "ware_ops_pipes.pipelines.subproblems.item_assignment.min_min_item_assignment",
            "MinMaxIA": "ware_ops_pipes.pipelines.subproblems.item_assignment.min_max_item_assignment",
            "SShape": "ware_ops_pipes.pipelines.subproblems.routing.s_shape",
            "NearestNeighbourhood": "ware_ops_pipes.pipelines.subproblems.routing.nn",
            "LargestGap": "ware_ops_pipes.pipelines.subproblems.routing.largest_gap",
            "Midpoint": "ware_ops_pipes.pipelines.subproblems.routing.midpoint",
            "Return": "ware_ops_pipes.pipelines.subproblems.routing.return_algo",
            "ExactSolving": "ware_ops_pipes.pipelines.subproblems.routing.exact_algo",
            "RatliffRosenthal": "ware_ops_pipes.pipelines.subproblems.routing.sprp",
            "RatliffRosenthalNF": "ware_ops_pipes.pipelines.subproblems.routing.rr_ss",
            "FiFo": "ware_ops_pipes.pipelines.subproblems.batching.fifo",
            "OrderNrFiFo": "ware_ops_pipes.pipelines.subproblems.batching.order_nr_fifo",
            "DueDate": "ware_ops_pipes.pipelines.subproblems.batching.due_date",
            "Random": "ware_ops_pipes.pipelines.subproblems.batching.random",
            "CombinedBatchingRoutingAssigning": "ware_ops_pipes.pipelines.subproblems.routing.joint_batching_routing_assigning",
            # "ClosestDepotMinDistanceSeedBatching": "ware_ops_pipes.pipelines.subproblems.batching.seed",
            # "ClosestDepotMaxSharedArticlesSeedBatching": "ware_ops_pipes.pipelines.subproblems.batching.seed_shared_articles",
            # "ClarkAndWrightSShape": "ware_ops_pipes.pipelines.subproblems.batching.clark_and_wright_sshape",
            # "ClarkAndWrightNN": "ware_ops_pipes.pipelines.subproblems.batching.clark_and_wright_nn",
            # "ClarkAndWrightRR": "ware_ops_pipes.pipelines.subproblems.batching.clark_and_wright_rr",
            # "LSBatchingRR": "ware_ops_pipes.pipelines.subproblems.batching.ls_rr",
            # "LSBatchingNNRand": "ware_ops_pipes.pipelines.subproblems.batching.ls_nn_rand",
            # "LSBatchingNNDueDate": "ware_ops_pipes.pipelines.subproblems.batching.ls_nn_due",
            # "LSBatchingNNFiFo": "ware_ops_pipes.pipelines.subproblems.batching.ls_nn_fifo",
            # "LSBatchingNNFiFoOrderNr": "ware_ops_pipes.pipelines.subproblems.batching.ls_nn_fifo_ord_nr",
            # "LSBatchingSShapeFiFoOrderNr": "ware_ops_pipes.pipelines.subproblems.batching.ls_sshape_fifo_ord_nr",
            "SPTScheduler": "ware_ops_pipes.pipelines.subproblems.scheduling.spt_scheduling",
            "LPTScheduler": "ware_ops_pipes.pipelines.subproblems.scheduling.lpt_scheduling",
            "EDDScheduler": "ware_ops_pipes.pipelines.subproblems.scheduling.edd_scheduling",
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
        timings = {}
        t0 = time.perf_counter()
        timings["load_domain"] = time.perf_counter() - t0

        # Filter applicable algorithms
        t0 = time.perf_counter()
        domain_algo_mapper = DomainAlgorithmMapper(TAXONOMY)
        algos_applicable = domain_algo_mapper.filter(
            algorithms=self.algos,
            instance=self.data_card,
            verbose=self.verbose
        )
        timings["filter_and_import"] = time.perf_counter() - t0

        if self.verbose:
            print(f"✓ {len(algos_applicable)}/{len(self.algos)} algorithms applicable")

        # Import applicable models
        final_algos = []
        for m in algos_applicable:
            if m.algo_name not in self.excluded:
                final_algos.append(m)

        self._import_models(final_algos)

        # Setup output folder
        output_folder = (
                self.project_root / "experiments" / "output"
                / self.instance_set_name / instance_name
        )
        output_folder.mkdir(parents=True, exist_ok=True)

        set_pipeline_params(
            output_folder=str(output_folder),
            data_cache_folder=str(
                self.project_root / "experiments" / "output" / "_data_cache"
            ),
            instance_set_name=self.instance_set_name,
            instance_name=instance_name,
            instance_path=str(file_paths[0]),
            instances_dir=str(self.instances_dir),
            loader_cls=self.loader_cls,
            loader_kwargs=self.loader_kwargs,
            time_limit_seconds=self.time_limit_sec,
            gen_tour=self.gen_tour,
        )

        # Build and run pipelines
        t0 = time.perf_counter()
        pipelines = None
        if len(algos_applicable) > 0:
            pipelines = self._build_pipelines()
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
            timings["run_pipelines"] = time.perf_counter() - t0
            timings["total"] = sum(timings.values())
            self.pipeline_runtimes[instance_name] = timings
        else:
            print("⚠ No valid pipelines found!")

    def _import_models(self, algos_applicable):
        """Import applicable concrete CLS-Luigi components."""

        for card in algos_applicable:
            try:
                configured_module = CONFIGURED_COMPONENT_MODULES.get(
                    card.algo_name
                )

                if configured_module is not None:
                    import_algo_class(
                        card.algo_name,
                        configured_module,
                    )

                    if self.verbose:
                        print(
                            f"✅ {card.algo_name} "
                            f"from {configured_module}"
                        )

                    continue

                module_path = self.implementation_module.get(
                    card.algo_name
                )
                if module_path is None:
                    if self.verbose:
                        print(
                            f"⚠ Unknown model: "
                            f"{card.algo_name}, skipping..."
                        )
                    continue
                import_algo_class(
                    card.algo_name,
                    module_path,
                )
                if self.verbose:
                    print(f"✅ {card.algo_name}")
            except Exception as exc:
                if self.verbose:
                    print(
                        f"❌ Failed to import "
                        f"{card.algo_name}: {exc}"
                    )

    def _build_pipelines(self):
        """Build valid pipelines using inhabitation"""
        from ware_ops_pipes.pipelines.templates.template_1 import (
            LayoutLoader, InstanceLoader, AbstractItemAssignment, AbstractBatching,
            MultiOrderBatching, AbstractPickerRouting,
            AbstractScheduling, AbstractResultAggregation
        )

        endpoint = AbstractResultAggregation
        repository = RepoMeta.repository
        fcl = FiniteCombinatoryLogic(repository, Subtypes(RepoMeta.subtypes))
        inhabitation_result, inhabitation_size = inhabit(endpoint)

        max_results = self.max_pipelines if inhabitation_size == 0 else inhabitation_size

        validator = UniqueTaskPipelineValidator([
            LayoutLoader,
            InstanceLoader,
            AbstractItemAssignment,
            AbstractBatching,
            MultiOrderBatching,
            AbstractPickerRouting,
            AbstractScheduling,
            AbstractResultAggregation
        ])

        print(f"Enumerating up to {max_results} pipelines...")
        pipelines = [
            t() for t in inhabitation_result.evaluated[0:max_results]
            if validator.validate(t())
        ]

        if self.verbose and pipelines:
            print(f"✓ Found {len(pipelines)} valid pipelines")
            for i, pipeline in enumerate(pipelines, 1):  # Show first 3
                print(f"\nPipeline {i}:")
                print(print_tree(pipeline))
        return pipelines

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
