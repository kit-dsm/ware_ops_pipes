import os
import pandas as pd
from cls_luigi.inhabitation_task import ClsParameter


from ware_ops_algos.algorithms.algorithm_interfaces import CombinedRoutingSolution, RoutingSolution, ItemAssignmentSolution, \
    BatchObject
from ware_ops_algos.algorithms import Batching, Routing, \
    BatchingSolution, ItemAssignment, RoutingBatchingAssigning, build_jobs, PriorityScheduler
from ware_ops_algos.domain_models import OrdersDomain, Resources, LayoutData, Articles, StorageLocations
from ware_ops_algos.domain_models.base_domain import BaseWarehouseDomain
from ware_ops_pipes.pipelines import BaseComponent
from ware_ops_pipes.pipelines.io_helpers import dump_pickle, load_pickle, load_json, dump_json
from ware_ops_pipes.synthesis.pipeline_provenance import collect_from_graph


class InstanceLoader(BaseComponent):
    abstract = False

    def output(self):
        return {
            "domain": self.get_luigi_local_target_with_task_id("domain.pkl"),
            "orders": self.get_luigi_local_target_with_task_id("orders.pkl"),
            "resources": self.get_luigi_local_target_with_task_id("resources.pkl"),
            "layout": self.get_luigi_local_target_with_task_id("layout.pkl"),
            "articles": self.get_luigi_local_target_with_task_id("articles.pkl"),
            "storage": self.get_luigi_local_target_with_task_id("storage.pkl"),
            "warehouse_info": self.get_luigi_local_target_with_task_id("warehouse_info.pkl"),
        }

    def run(self):
        domain_path = self.pipeline_params.domain_path
        print("Path", domain_path)
        if not domain_path:
            raise ValueError("Pipeline parameter 'domain_path' is not set.")

        # Load cached domain object
        domain: BaseWarehouseDomain = load_pickle(domain_path)
        for target in self.output().values():
            os.makedirs(os.path.dirname(target.path), exist_ok=True)
        dump_pickle(self.output()["domain"].path, domain)
        dump_pickle(self.output()["orders"].path, domain.orders)
        dump_pickle(self.output()["resources"].path, domain.resources)
        dump_pickle(self.output()["layout"].path, domain.layout)
        dump_pickle(self.output()["articles"].path, domain.articles)
        dump_pickle(self.output()["storage"].path, domain.storage)
        dump_pickle(self.output()["warehouse_info"].path, domain.warehouse_info)


class AbstractItemAssignment(BaseComponent):
    abstract = True
    instance = ClsParameter(tpe=InstanceLoader.return_type())

    def get_inited_item_assigner(self) -> ItemAssignment:
        ...

    def requires(self):
        return {
            "instance": self.instance(),
        }

    def output(self):
        return {
            "item_assignment_sol": self.get_luigi_local_target_with_task_id(
                "item_assignment_sol.pkl"
            )
        }

    def run(self):
        orders_domain = load_pickle(self.input()["instance"]["orders"].path)
        selector = self.get_inited_item_assigner()

        ia_sol = selector.solve(orders_domain.orders)
        orders_domain.orders = ia_sol.resolved_orders
        dump_pickle(self.output()["item_assignment_sol"].path, ia_sol)


class AbstractBatching(BaseComponent):
    abstract = True
    instance = ClsParameter(tpe=InstanceLoader.return_type())
    item_assignment_sol = ClsParameter(tpe=AbstractItemAssignment.return_type())

    def requires(self):
        return {
            "instance": self.instance(),
            "item_assignment_sol": self.item_assignment_sol()
        }

    def output(self):
        return {
            "batching_sol": self.get_luigi_local_target_with_task_id(
                "batching_sol.pkl"
            )
        }


class SingleOrderBatching(AbstractBatching):
    abstract = False

    def run(self):
        ia_sol: ItemAssignmentSolution = load_pickle(
            self.input()["item_assignment_sol"]["item_assignment_sol"].path
        )

        resolved_orders = ia_sol.resolved_orders

        batches = []
        for order in resolved_orders:
            batch = BatchObject(batch_id=0, orders=[order])
            batches.append(batch)

        batching_solution = BatchingSolution(batches=batches)
        batching_solution.execution_time = 0
        batching_solution.algo_name = "RawInput"

        dump_pickle(
            self.output()["batching_sol"].path,
            batching_solution,
        )


class MultiOrderBatching(AbstractBatching):
    abstract = True

    def get_inited_batcher(self) -> Batching:
        ...

    def run(self):
        batcher: Batching = self.get_inited_batcher()

        ia_sol: ItemAssignmentSolution = load_pickle(
            self.input()["item_assignment_sol"]["item_assignment_sol"].path
        )
        resolved_orders = ia_sol.resolved_orders

        batching_sol: BatchingSolution = batcher.solve(resolved_orders)

        if batcher.__class__.__name__ in [
            "SeedBatching",
            "ClarkAndWrightBatching",
            "LocalSearchBatching",
        ]:
            batching_algo_name = batcher.algo_name
        else:
            batching_algo_name = batcher.__class__.__name__

        batching_sol.algo_name = batching_algo_name

        dump_pickle(
            self.output()["batching_sol"].path,
            batching_sol,
        )


class AbstractPickerRouting(BaseComponent):
    abstract = True
    instance = ClsParameter(tpe=InstanceLoader.return_type())

    def requires(self):
        return {
            "instance": self.instance(),
        }

    def output(self):
        return {
            "routing_sol": self.get_luigi_local_target_with_task_id(
                "routing_sol.pkl")
        }

    def _get_inited_router(self, start_node: tuple[float, float] | None = None) -> Routing:
        ...

    def _load_resources(self) -> Resources:
        return load_pickle(self.input()["instance"]["resources"].path)

    def _load_storage(self) -> StorageLocations:
        return load_pickle(self.input()["instance"]["storage"].path)

    def _load_routing_data(self):
        return load_pickle(self.input()["routing_input"]["routing_input"].path)

    def _load_layout(self) -> LayoutData:
        return load_pickle(self.input()["instance"]["layout"].path)

    def _load_articles(self) -> Articles:
        return load_pickle(self.input()["instance"]["articles"].path)

    def _load_config(self):
        return load_json(self.input()["routing_config"]["routing_config"].path)


class PickerRouting(AbstractPickerRouting):
    abstract = True
    batching_sol = ClsParameter(tpe=AbstractBatching.return_type())

    def requires(self):
        return {
            "instance": self.instance(),
            "batching_sol": self.batching_sol(),
        }

    def run(self):
        router: Routing = self._get_inited_router()

        batching_sol: BatchingSolution = load_pickle(
            self.input()["batching_sol"]["batching_sol"].path
        )

        routing_sols = []

        for i, batch in enumerate(batching_sol.batches):
            routing_solution = router.solve(batch.pick_positions)
            routing_solution.route.batch = batch
            routing_sols.append(routing_solution)
            router.reset_parameters()

        dump_pickle(
            self.output()["routing_sol"].path,
            routing_sols,
        )


class CombinedIAR(AbstractPickerRouting):
    abstract = True

    def requires(self):
        return {
            "instance": self.instance()
        }

    def run(self):
        orders_domain: OrdersDomain = load_pickle(self.input()["instance"]["orders"].path)
        orders = orders_domain.orders
        router: Routing = self._get_inited_router()
        routing_sols = []
        pick_lists = []
        for order in orders:
            pl = []
            for pp in order.order_positions:
                pl.append(pp)
            pick_lists.append(pl)
        for i, pick_list in enumerate(pick_lists):
            routing_solution = router.solve(pick_list)
            # routing_solution.route.pick_list = pl
            routing_sols.append(routing_solution)

            router.reset_parameters()

        dump_pickle(self.output()["routing_sol"].path, routing_sols)


class CombinedBR(AbstractPickerRouting):
    abstract = True
    item_assignment_sol = ClsParameter(tpe=AbstractItemAssignment.return_type())

    def requires(self):
        return {
            "instance": self.instance(),
            "item_assignment_sol": self.item_assignment_sol()
        }

    def output(self):
        return {
            "routing_sol": self.get_luigi_local_target_with_task_id(
                "routing_sol.pkl")
        }

    def run(self):
        ia_sol: ItemAssignmentSolution = load_pickle(self.input()["item_assignment_sol"]["item_assignment_sol"].path)
        resolved_orders = ia_sol.resolved_orders

        router = self._get_inited_router()
        combined_solution: CombinedRoutingSolution = router.solve(resolved_orders)
        dump_pickle(self.output()["routing_sol"].path, combined_solution)

    def _get_inited_router(self, start_node: tuple[float, float] | None = None) -> RoutingBatchingAssigning:
        ...


class AbstractScheduling(BaseComponent):
    abstract = True
    routing_sol = ClsParameter(tpe=AbstractPickerRouting.return_type())
    instance = ClsParameter(tpe=InstanceLoader.return_type())

    def requires(self):
        return {
            "routing_sol": self.routing_sol(),
            "instance": self.instance(),
        }

    def output(self):
        return {
            "scheduling_sol": self.get_luigi_local_target_with_task_id(
                "scheduling_sol.pkl"
            )
        }

    def run(self):
        routing_solutions = load_pickle(
            self.input()["routing_sol"]["routing_sol"].path
        )
        resources = self._load_resources()

        if isinstance(routing_solutions, CombinedRoutingSolution):
            routes = routing_solutions.routes
        else:
            routes = [routing_solution.route for routing_solution in routing_solutions]

        jobs = build_jobs(routes, resources)
        scheduler = self._get_inited_scheduler()
        scheduling_solution = scheduler.solve(jobs)
        scheduler_algo_name = scheduler.__class__.__name__

        scheduling_solution.algo_name = scheduler_algo_name

        dump_pickle(
            self.output()["scheduling_sol"].path,
            scheduling_solution,
        )

    def _get_inited_scheduler(self) -> PriorityScheduler:
        ...

    def _load_resources(self) -> Resources:
        return load_pickle(self.input()["instance"]["resources"].path)

    def _load_orders(self) -> OrdersDomain:
        return load_pickle(self.input()["instance"]["orders"].path)

    def _load_routing_solution(self):
        return load_pickle(self.input()["routing_sol"]["routing_sol"].path)


class AbstractResultAggregation(BaseComponent):
    abstract = True

    def output(self):
        return {
            "summary": self.get_luigi_local_target_with_task_id(
                "summary.json"
            )
        }

    def _collect(self) -> dict[str, dict]:
        """Walk own task graph and collect all stage solutions + metadata."""
        return collect_from_graph(self)

    def _build_provenance_summary(self, summary: dict) -> dict[str, dict]:
        """Populate summary with instance info and provenance. Returns collected stages."""
        collected = self._collect()

        summary["instance_name"] = self.pipeline_params.instance_name
        summary["instance_set"] = self.pipeline_params.instance_set_name

        # Write provenance as a flat list (for serialisation) and per-stage keys
        provenance_list = []
        for stage_name in [
            "item_assignment", "batching", "routing",
            "assignment", "scheduling", "scheduling",
        ]:
            if stage_name in collected:
                entry = collected[stage_name]
                provenance_list.append({
                    "stage": stage_name,
                    "algo": entry["algo"],
                    "time": entry["time"],
                    "task_class": entry["task_class"],
                })
                summary[f"{stage_name}_algo"] = entry["algo"]
                summary[f"{stage_name}_time"] = entry["time"]

        summary["provenance"] = provenance_list
        return collected

    @staticmethod
    def _compute_tour_summary_from_list(routing_sols: list[RoutingSolution]) -> dict:
        """Build tour distance summary from a list of RoutingSolution."""
        tour_distances = {}
        routing_times = {}
        total_distance = 0

        for tour_id, sol in enumerate(routing_sols):
            routing_times[f"tour_{tour_id}_time"] = sol.execution_time
            distance = sol.route.distance
            tour_distances[f"tour_{tour_id}_distance"] = distance
            total_distance += distance

        return {
            "tour_distances": tour_distances,
            "total_distance": total_distance,
            "time_per_tour": routing_times,
        }

    @staticmethod
    def _compute_tour_summary_from_combined(combined_sol: CombinedRoutingSolution) -> dict:
        """Build tour distance summary from a CombinedRoutingSolution."""
        tour_distances = {}
        total_distance = 0
        for tour_id, sol in enumerate(combined_sol.routes):
            distance = sol.distance
            tour_distances[f"tour_{tour_id}_distance"] = distance
            total_distance += distance

        return {
            "tour_distances": tour_distances,
            "total_distance": total_distance,
            "execution_time": combined_sol.execution_time,
        }


class ResultAggregationDistance(AbstractResultAggregation):
    abstract = False
    routing_sol = ClsParameter(tpe=AbstractPickerRouting.return_type())

    def requires(self):
        return {
            "routing_sol": self.routing_sol(),
        }

    def run(self):
        summary: dict = {}
        collected = self._build_provenance_summary(summary)

        routing_entry = collected.get("routing")
        if routing_entry is None:
            raise ValueError("No routing solution found in the task graph.")

        sol = routing_entry["solution"]

        if isinstance(sol, list):
            summary["tours_summary"] = self._compute_tour_summary_from_list(sol)
        elif isinstance(sol, CombinedRoutingSolution):
            summary["tours_summary"] = self._compute_tour_summary_from_combined(sol)

        dump_json(self.output()["summary"].path, summary)


class ResultAggregationDueDate(AbstractResultAggregation):
    abstract = False
    scheduling_sol = ClsParameter(tpe=AbstractScheduling.return_type())
    instance = ClsParameter(tpe=InstanceLoader.return_type())

    def requires(self):
        return {
            "scheduling_sol": self.scheduling_sol(),
            "instance": self.instance(),
        }

    def _evaluate_due_dates(self, scheduled_jobs, orders: OrdersDomain) -> pd.DataFrame:
        order_by_id = {o.order_id: o for o in orders.orders}
        records = []

        for sj in scheduled_jobs:
            job = sj.job

            for order_number in job.order_numbers:
                order = order_by_id.get(order_number)
                if order is None:
                    continue
                if order.due_date is None:
                    continue

                lateness = sj.end_time - order.due_date

                records.append({
                    "order_number": order_number,
                    "job_id": job.job_id,
                    "picker_id": sj.picker_id,
                    "arrival_time": order.order_date,
                    "release_time": job.release_time,
                    "start_time": sj.start_time,
                    "completion_time": sj.end_time,
                    "due_date": order.due_date,
                    "processing_time": job.processing_time,
                    "distance": job.distance,
                    "n_picks": job.n_picks,
                    "lateness": lateness,
                    "tardiness": max(0.0, lateness),
                    "on_time": sj.end_time <= order.due_date,
                })

        return pd.DataFrame(records)

    @staticmethod
    def _scheduled_jobs_to_frame(scheduled_jobs) -> pd.DataFrame:
        records = []

        for sj in scheduled_jobs:
            job = sj.job

            records.append({
                "job_id": job.job_id,
                "picker_id": sj.picker_id,
                "release_time": job.release_time,
                "start_time": sj.start_time,
                "end_time": sj.end_time,
                "processing_time": job.processing_time,
                "due_date": job.due_date,
                "lateness": sj.lateness,
                "tardiness": sj.tardiness,
                "on_time": sj.is_on_time,
                "distance": job.distance,
                "n_picks": job.n_picks,
            })

        return pd.DataFrame(records)

    def run(self):
        summary: dict = {}
        collected = self._build_provenance_summary(summary)

        # --- Routing distances from graph ---
        routing_entry = collected.get("routing")
        if routing_entry is not None:
            sol = routing_entry["solution"]
            if isinstance(sol, list):
                summary["tours_summary"] = self._compute_tour_summary_from_list(sol)
            elif isinstance(sol, CombinedRoutingSolution):
                summary["tours_summary"] = self._compute_tour_summary_from_combined(sol)

        # --- Scheduling / due-date ranking ---
        scheduling_entry = collected.get("scheduling")
        if scheduling_entry is None:
            raise ValueError("No scheduling solution found in the task graph.")

        scheduling_sol = scheduling_entry["solution"]
        scheduled_jobs = scheduling_sol.jobs

        orders: OrdersDomain = load_pickle(
            self.input()["instance"]["orders"].path
        )

        due_eval = self._evaluate_due_dates(scheduled_jobs, orders)

        if due_eval.empty:
            summary["makespan"] = 0.0
            summary["on_time_rate"] = 0.0
            summary["avg_lateness"] = 0.0
            summary["avg_tardiness"] = 0.0
            summary["max_lateness"] = 0.0
            summary["max_tardiness"] = 0.0
            dump_json(self.output()["summary"].path, summary)
            return

        summary["on_time_rate"] = float(due_eval["on_time"].mean() * 100.0)
        summary["avg_lateness"] = float(due_eval["lateness"].mean())
        summary["avg_tardiness"] = float(due_eval["tardiness"].mean())
        summary["max_lateness"] = float(due_eval["lateness"].max())
        summary["max_tardiness"] = float(due_eval["tardiness"].max())

        df_jobs = self._scheduled_jobs_to_frame(scheduled_jobs)

        summary["makespan"] = float(df_jobs["end_time"].max())

        picker_util = (
            df_jobs.groupby("picker_id")["processing_time"]
            .sum()
            .to_dict()
        )
        summary["picker_processing_time"] = {
            str(k): float(v) for k, v in picker_util.items()
        }

        dump_json(self.output()["summary"].path, summary)



if __name__ == "__main__":
    from luigi.task import flatten

    from pathlib import Path
    from ware_ops_algos.data_loaders import HesslerIrnichLoader

    from ware_ops_pipes.synthesis.runner import PipelineRunner, collect_from_graph


    class HesslerIrnichRunner(PipelineRunner):
        """Runner for Hessler-Irnich format instances"""

        def __init__(self, instance_set_name: str, instances_dir: Path, cache_dir: Path,
                     project_root: Path, **kwargs):
            super().__init__(instance_set_name, instances_dir, cache_dir, project_root, **kwargs)
            self.loader = HesslerIrnichLoader(str(instances_dir), str(cache_dir))

        def discover_instances(self) -> list[tuple[str, list[Path]]]:
            instances = []
            for filepath in self.instances_dir.glob("*.txt"):
                if filepath.is_file():
                    instances.append((filepath.stem, [filepath]))
            return instances

        def load_domain(self, instance_name: str, file_paths: list[Path]) -> BaseWarehouseDomain:
            return self.loader.load(file_paths[0].name, use_cache=True)

    def print_tree(task, indent='', last=True):
        name = task.__class__.__name__
        result = '\n' + indent
        if last:
            result += '└─--'
            indent += '    '
        else:
            result += '|---'
            indent += '|   '
        result += '[{0}]'.format(name)
        children = flatten(task.requires())
        for index, child in enumerate(children):
            result += print_tree(child, indent, (index + 1) == len(children))
        return result

    PROJECT_ROOT = Path(__file__).parent.parent
    DATA_DIR = PROJECT_ROOT / "data"

    instances_base = DATA_DIR / "instances"
    cache_base = DATA_DIR / "instances" / "caches"

    instance_set = "SPRP"  # SPRP-SS

    runner = HesslerIrnichRunner(instance_set, instances_base / instance_set,
                                 cache_base / instance_set, PROJECT_ROOT, verbose=True)

    runner.run_all()
