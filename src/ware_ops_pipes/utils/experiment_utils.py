from __future__ import annotations

import json
import pickle
import time
from abc import abstractmethod, ABC
from typing import Tuple, Dict
from pathlib import Path

import pandas as pd
import luigi
import ware_ops_algos
from cls.fcl import FiniteCombinatoryLogic
from cls.subtypes import Subtypes
from cls_luigi.inhabitation_task import RepoMeta
from cls_luigi.unique_task_pipeline_validator import UniqueTaskPipelineValidator

from ware_ops_algos.data_loaders import DataLoader
from ware_ops_algos.domain_models.base_domain import BaseWarehouseDomain
from ware_ops_algos.domain_models.taxonomy import SUBPROBLEMS
from ware_ops_algos.algorithms.algorithm_filter import AlgorithmFilter
from ware_ops_algos.utils.general_functions import import_model_class, load_model_cards
from ware_ops_pipes.utils.io_helpers import load_json
from ware_ops_pipes.pipelines import set_pipeline_params, inhabit, print_tree

Node = tuple[float, float]


class RankingEvaluator(ABC):
    """
    Class to evaluate output of a Pipeline Runner.
    Evaluates and outputs a batch of pipelines.
    """
    def __init__(self, output_dir: str, instance_name: str):
        self.output_dir = Path(output_dir)
        self.instance_name = instance_name

    @abstractmethod
    def evaluate(self):
        pass


class RankingEvaluatorDistance(RankingEvaluator):
    def __init__(self, output_dir: str, instance_name: str):

        super().__init__(output_dir, instance_name)
        self.df_result = None

    def evaluate(self, metric_path: str = "tours_summary.total_distance",
                 minimize: bool = True) -> pd.DataFrame:
        """
        Rank pipelines by metric.

        Args:
            metric_path: Metric to rank by (e.g., 'tours_summary.total_distance')
            minimize: True if lower is better

        Returns:
            DataFrame with ranked results
        """
        # Collect all summaries
        results = []
        for file in self.output_dir.glob("*summary.json"):
            summary = load_json(str(file))

            # Extract metric
            metric_value = self._get_metric(summary, metric_path)
            if metric_value is None:
                continue

            # Extract pipeline info
            pipeline_id = f"{summary.get('item_assignment_algo')}+{summary.get('batching_algo')}+{summary.get('routing_algo')}"

            results.append({
                "pipeline_id": pipeline_id,
                "item_assignment_algo": summary.get('item_assignment_algo'),
                "batching_algo": summary.get('batching_algo'),
                "routing_algo": summary.get('routing_algo'),
                "value": metric_value,
            })

        if not results:
            print(f"No results found in {self.output_dir}")
            return pd.DataFrame()

        # Sort and rank
        df = pd.DataFrame(results)
        df = df.sort_values("value", ascending=minimize).reset_index(drop=True)
        df['rank'] = range(1, len(df) + 1)

        # Calculate gap to best
        best = df.iloc[0]['value']
        df['gap_to_best'] = df['value'] - best
        df['gap_pct'] = ((df['value'] - best) / best * 100.0) if best != 0 else 0

        # Save
        output_file = self.output_dir / f"ranking_{metric_path.replace('.', '_')}.csv"
        df.to_csv(output_file, index=False)

        # Print top 5
        print(f"\nTop 5 pipelines for {self.instance_name}:")
        print(df[['rank', 'pipeline_id', 'value', 'gap_pct']].head().to_string(index=False))
        print(f"\nSaved: {output_file}\n")

        self.df_result = df
        return df

    def _get_metric(self, summary: Dict, metric_path: str):
        """Extract metric using dot notation"""
        if '.' in metric_path:
            parts = metric_path.split('.')
            value = summary
            for part in parts:
                if isinstance(value, dict) and part in value:
                    value = value[part]
                else:
                    return None
            return value
        return summary.get(metric_path)


class RankingEvaluatorSequencing(RankingEvaluator):
    def __init__(self, output_dir: str, instance_name: str):

        super().__init__(output_dir, instance_name)

    @staticmethod
    def load_sequencing_solutions(base_dir: str):
        sol_files = Path(base_dir).glob("**/*scheduling_plan.pkl")
        solutions = {}
        for f in sol_files:
            with open(f, "rb") as fh:
                try:
                    solutions[f.name] = pickle.load(fh)
                except Exception as e:
                    print(f"❌ Failed to load {f}: {e}")
        return solutions

    def evaluate(self, metric_path: str = "tours_summary.due_dates",
                 minimize: bool = True) -> pd.DataFrame:
        """
        Rank pipelines by metric.

        Args:
            metric_path: Metric to rank by (e.g., 'tours_summary.total_distance')
            minimize: True if lower is better

        Returns:
            DataFrame with ranked results
        """
        # Collect all summaries
        solutions = self.load_sequencing_solutions(str(self.output_dir))

        best_key, best_dist = None, float("inf")
        for k, plan in solutions.items():
            dist = sum(a.distance for a in plan.sequencing_solutions.jobs)
            if dist < best_dist:
                best_key, best_dist = k, dist
        print(best_key)
        solution = solutions[best_key].sequencing_solutions
        # df = self._evaluate_due_dates(solution)
        return solution

    # def _evaluate_due_dates(self, assignments: list[Job]):
    #     # order_by_id = {o.order_id: o for o in orders}
    #     records = []
    #     for ass in assignments:
    #         end_time = ass.end_time
    #         for on in ass.route.pick_list.order_numbers:
    #             # o = order_by_id.get(on)
    #             # if o is None:
    #             #     continue
    #             # if o.due_date is None:
    #             #     continue  # skip if no due date
    #
    #             arrival_time = o.order_date
    #             start_time = ass.start_time
    #             due_ts = o.due_date  # .timestamp()
    #             lateness = end_time - due_ts
    #             records.append({
    #                 "order_number": on,
    #                 "arrival_time": arrival_time,
    #                 "start_time": start_time,
    #                 "picker_id": ass.picker_id,
    #                 "completion_time": end_time,
    #                 "due_date": o.due_date,
    #                 "lateness": lateness,
    #                 "tardiness": max(0, lateness),
    #                 "on_time": end_time <= due_ts,
    #             })
    #     return pd.DataFrame(records)

    def _get_metric(self, summary: Dict, metric_path: str):
        """Extract metric using dot notation"""
        if '.' in metric_path:
            parts = metric_path.split('.')
            value = summary
            for part in parts:
                if isinstance(value, dict) and part in value:
                    value = value[part]
                else:
                    return None
            return value
        return summary.get(metric_path)


class PipelineRunner(ABC):
    """Base class for running pipelines on warehouse instances"""

    def __init__(
            self,
            instance_set_name: str,
            instances_dir: Path,
            cache_dir: Path,
            project_root: Path,
            max_pipelines: int = 10,
            verbose: bool = True,
            cleanup: bool = True,
            ranker=RankingEvaluatorDistance,
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

        # Component implementations
        self.implementation_module = {
            "GreedyIA": "ware_ops_pipes.pipelines.components.item_assignment.greedy_item_assignment",
            "NNItemAssignment": "ware_ops_pipes.pipelines.components.item_assignment.nn_item_assignment",
            "DummyOS": "ware_ops_pipes.pipelines.components.order_selection.dummy_order_selection",
            "MinMaxArticlesOS": "ware_ops_pipes.pipelines.components.order_selection.min_max_articles_os",
            "MinMaxAislesOS": "ware_ops_pipes.pipelines.components.order_selection.min_max_aisles_os",
            "GreedyOS": "ware_ops_pipes.pipelines.components.order_selection.greedy_order_selection",
            "MinSharedAislesOS": "ware_ops_pipes.pipelines.components.order_selection.min_shared_aisles_os",
            "SShape": "ware_ops_pipes.pipelines.components.routing.s_shape",
            "NearestNeighbourhood": "ware_ops_pipes.pipelines.components.routing.nn",
            "PLRouting": "ware_ops_pipes.pipelines.components.routing.pl",
            "LargestGap": "ware_ops_pipes.pipelines.components.routing.largest_gap",
            "Midpoint": "ware_ops_pipes.pipelines.components.routing.midpoint",
            "Return": "ware_ops_pipes.pipelines.components.routing.return_algo",
            # "ExactSolving": "ware_ops_pipes.pipelines.components.routing.exact_algo",
            "RatliffRosenthal": "ware_ops_pipes.pipelines.components.routing.sprp",
            "FiFo": "ware_ops_pipes.pipelines.components.batching.fifo",
            "OrderNrFiFo": "ware_ops_pipes.pipelines.components.batching.order_nr_fifo",
            "DueDate": "ware_ops_pipes.pipelines.components.batching.due_date",
            "Random": "ware_ops_pipes.pipelines.components.batching.random",
            # "CombinedBatchingRoutingAssigning": "ware_ops_pipes.pipelines.components.routing.joint_batching_routing_assigning",
            "ClosestDepotMinDistanceSeedBatching": "ware_ops_pipes.pipelines.components.batching.seed",
            "ClosestDepotMaxSharedArticlesSeedBatching": "ware_ops_pipes.pipelines.components.batching.seed_shared_articles",
            "ClarkAndWrightSShape": "ware_ops_pipes.pipelines.components.batching.clark_and_wright_sshape",
            "ClarkAndWrightNN": "ware_ops_pipes.pipelines.components.batching.clark_and_wright_nn",
            "ClarkAndWrightRR": "ware_ops_pipes.pipelines.components.batching.clark_and_wright_rr",
            "LSBatchingRR": "ware_ops_pipes.pipelines.components.batching.ls_rr",
            "LSBatchingNNRand": "ware_ops_pipes.pipelines.components.batching.ls_nn_rand",
            "LSBatchingNNDueDate": "ware_ops_pipes.pipelines.components.batching.ls_nn_due",
            "LSBatchingNNFiFo": "ware_ops_pipes.pipelines.components.batching.ls_nn_fifo",
            "SPTScheduling": "ware_ops_pipes.pipelines.components.sequencing.spt_scheduling",
            "LPTScheduling": "ware_ops_pipes.pipelines.components.sequencing.lpt_scheduling",
            "EDDScheduling": "ware_ops_pipes.pipelines.components.sequencing.edd_scheduling",
            "EDDSequencing": "ware_ops_pipes.pipelines.components.sequencing.edd_sequencing",
            "RRAssigner": "ware_ops_pipes.pipelines.components.picker_assignment.round_robin_assignment"
        }
        pkg_dir = Path(ware_ops_algos.__file__).parent
        model_cards_path = pkg_dir / "algorithms" / "algorithm_cards"
        self.models = load_model_cards(str(model_cards_path))
        if self.verbose:
            print(f"Loaded {len(self.models)} model cards")

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
        timings = {}
        # Load domain (with caching)
        t0 = time.perf_counter()
        domain = self.load_domain(instance_name, file_paths)
        timings["load_domain"] = time.perf_counter() - t0

        # Filter applicable algorithms
        t0 = time.perf_counter()
        algo_filter = AlgorithmFilter(SUBPROBLEMS)
        models_applicable = algo_filter.filter(
            algorithms=self.models,
            instance=domain,
            verbose=self.verbose
        )
        timings["filter_and_import"] = time.perf_counter() - t0

        if self.verbose:
            print(f"✓ {len(models_applicable)}/{len(self.models)} algorithms applicable")

        # Import applicable models
        self._import_models(models_applicable)

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
            domain_path=str(self.loader.cache_path)
        )

        # Build and run pipelines
        t0 = time.perf_counter()
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
                                                         'log_level': 'CRITICAL'  # <<<<<<<<<<
                                                         }))
            luigi.build(pipelines, local_scheduler=True)

            self.create_ranking(instance_name, output_folder)
            if self.cleanup:
                self._cleanup(output_folder)
            timings["run_pipelines"] = time.perf_counter() - t0
            timings["total"] = sum(timings.values())
            self.pipeline_runtimes[instance_name] = timings
        else:
            print("⚠ No valid pipelines found!")

    def _import_models(self, models_applicable):
        """Import applicable model implementations"""
        for model in models_applicable:
            model_name = model.model_name
            if model_name not in self.implementation_module:
                if self.verbose:
                    print(f"⚠ Unknown model: {model_name}, skipping...")
                continue

            try:
                module_path = self.implementation_module[model_name]
                cls = import_model_class(model_name, module_path)
                if self.verbose:
                    print(f"✅ {model_name}")
            except Exception as e:
                if self.verbose:
                    print(f"❌ Failed to import {model_name}: {e}")

    def _build_pipelines(self):
        """Build valid pipelines using inhabitation"""
        from ware_ops_pipes.pipelines.templates.template_1 import (
            InstanceLoader, AbstractItemAssignment, AbstractOrderSelection, AbstractPickListGeneration,
            BatchedPickListGeneration, AbstractPickerAssignment, AbstractPickerRouting, AbstractSequencing,
            AbstractScheduling, AbstractResultAggregation
        )

        endpoint = AbstractResultAggregation
        repository = RepoMeta.repository
        fcl = FiniteCombinatoryLogic(repository, Subtypes(RepoMeta.subtypes))
        inhabitation_result, inhabitation_size = inhabit(endpoint)

        max_results = self.max_pipelines if inhabitation_size == 0 else inhabitation_size

        validator = UniqueTaskPipelineValidator([
            InstanceLoader,
            AbstractItemAssignment,
            AbstractPickListGeneration,
            AbstractOrderSelection,
            BatchedPickListGeneration,
            AbstractPickerAssignment,
            AbstractPickerRouting,
            AbstractSequencing,
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
            for i, pipeline in enumerate(pipelines[:3], 1):  # Show first 3
                print(f"\nPipeline {i}:")
                print(print_tree(pipeline))
        return pipelines

    def _cleanup(self, output_folder: Path):
        """Clean up intermediate files"""
        try:
            for file_path in output_folder.glob("InstanceLoader__*.pkl"):
                file_path.unlink()
                if self.verbose:
                    print(f"🗑 Deleted {file_path.name}")
        except Exception as e:
            print(f"⚠ Cleanup failed: {e}")

    def create_ranking(self, instance_name: str, output_folder: Path):
        """Create ranking for this instance"""
        try:
            # ranker = RankingEvaluatorDistance(
            #     output_dir=str(output_folder),
            #     instance_name=instance_name
            # )

            ranker = self.ranker(
                output_dir=str(output_folder),
                instance_name=instance_name
            )
            # Rank by distance
            df = ranker.evaluate("tours_summary.total_distance", minimize=True)

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

