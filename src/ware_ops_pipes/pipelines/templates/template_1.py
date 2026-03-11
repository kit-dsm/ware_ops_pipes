import os
import pandas as pd
from cls_luigi.inhabitation_task import ClsParameter


from ware_ops_algos.algorithms.algorithm import PlanningState, CombinedRoutingSolution, RoutingSolution, ItemAssignmentSolution
from ware_ops_algos.algorithms import Batching, Routing, \
    BatchingSolution, ItemAssignment, RoutingBatchingAssigning, Assigner
from ware_ops_algos.algorithms.batching.batching_utils import build_pick_lists
from ware_ops_algos.algorithms.scheduling.scheduling import PriorityScheduling, SchedulingInput, PickListSequencer
from ware_ops_algos.domain_models import OrdersDomain, Resources, LayoutData, Articles
from ware_ops_algos.domain_models.base_domain import BaseWarehouseDomain
from ware_ops_pipes.pipelines import BaseComponent
from ware_ops_pipes.utils.io_helpers import dump_pickle, load_pickle, load_json, dump_json
from ware_ops_pipes.utils.experiment_utils import collect_from_graph

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
        plan = PlanningState(
            item_assignment=ia_sol,
        )

        algo_name = selector.__class__.__name__

        plan.provenance["item_assignment"] = {
            "algo": algo_name,
            "time": ia_sol.execution_time,
        }
        dump_pickle(self.output()["item_assignment_sol"].path, ia_sol)


class AbstractPickListGeneration(BaseComponent):
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
            "pick_list_sol": self.get_luigi_local_target_with_task_id(
                "pick_list_sol.pkl"
            )
        }


class RawPickListGeneration(AbstractPickListGeneration):
    abstract = False

    def run(self):
        ia_sol: ItemAssignmentSolution = load_pickle(self.input()["item_assignment_sol"]["item_assignment_sol"].path)
        resolved_orders = ia_sol.resolved_orders
        pick_lists = []
        for order in resolved_orders:
            pl = build_pick_lists([order])
            pick_lists.append(pl)

        batching_solution = BatchingSolution(pick_lists=pick_lists)
        batching_solution.execution_time = 0
        batching_solution.algo_name = "RawInput"

        dump_pickle(self.output()["pick_list_sol"].path, batching_solution)


class BatchedPickListGeneration(AbstractPickListGeneration):
    abstract = True

    def get_inited_batcher(self) -> Batching:
        ...

    def run(self):
        batcher: Batching = self.get_inited_batcher()
        ia_sol: ItemAssignmentSolution = load_pickle(self.input()["item_assignment_sol"]["item_assignment_sol"].path)
        resolved_orders = ia_sol.resolved_orders

        batching_sol = batcher.solve(resolved_orders)

        batches = batching_sol.batches
        pick_lists = []
        for batch in batches:
            pl = build_pick_lists(batch.orders)
            pick_lists.append(pl)

        batching_sol.pick_lists = pick_lists

        if batcher.__class__.__name__ in ["SeedBatching", "ClarkAndWrightBatching", "LocalSearchBatching"]:
            batching_algo_name = batcher.algo_name
        else:
            batching_algo_name = batcher.__class__.__name__

        batching_sol.algo_name = batching_algo_name

        dump_pickle(self.output()["pick_list_sol"].path, batching_sol)


class AbstractSequencing(BaseComponent):
    abstract = True
    instance = ClsParameter(tpe=InstanceLoader.return_type())
    pick_list_sol = ClsParameter(tpe=AbstractPickListGeneration.return_type())

    def requires(self):
        return {
            "instance": self.instance(),
            "pick_list_sol": self.pick_list_sol()
        }

    def output(self):
        return {
            "sequencing_sol": self.get_luigi_local_target_with_task_id(
                "sequencing_sol.pkl")
        }

    def _get_inited_sequencer(self) -> PickListSequencer:
        ...

    def _load_resources(self) -> Resources:
        return load_pickle(self.input()["instance"]["resources"].path)

    def _load_orders(self) -> OrdersDomain:
        return load_pickle(self.input()["instance"]["orders"].path)


class RoutingNeutralSequencing(AbstractSequencing):
    abstract = True

    def requires(self):
        return {
            "instance": self.instance(),
            "pick_list_sol": self.pick_list_sol()
        }

    def run(self):
        sequencer = self._get_inited_sequencer()
        pick_list_sol: BatchingSolution = load_pickle(self.input()["pick_list_sol"]["pick_list_sol"].path)
        pick_lists = pick_list_sol.pick_lists
        sequencing_sol = sequencer.solve(pick_lists)

        dump_pickle(self.output()["sequencing_sol"].path, sequencing_sol)


class AbstractPickerAssignment(BaseComponent):
    abstract = True
    instance = ClsParameter(tpe=InstanceLoader.return_type())
    pick_list_sol = ClsParameter(tpe=AbstractPickListGeneration.return_type())

    def requires(self):
        return {
            "instance": self.instance(),
            "pick_list_sol": self.pick_list_sol()
        }

    def output(self):
        return {
            "assignment_sol": self.get_luigi_local_target_with_task_id(
                "assignment_sol.pkl")
        }

    def _get_inited_assigner(self) -> Assigner:
        ...

    def _load_resources(self) -> Resources:
        return load_pickle(self.input()["instance"]["resources"].path)

    def _load_orders(self) -> OrdersDomain:
        return load_pickle(self.input()["instance"]["orders"].path)

    def run(self):
        plan: PlanningState = load_pickle(self.input()["pick_list_plan"]["pick_list_plan"].path)
        pick_lists = plan.batching_solutions.pick_lists
        assigner = self._get_inited_assigner()
        assignment_sol = assigner.solve(pick_lists)

        plan.provenance["routing_input"] = {
            "algo": assigner.__class__.__name__,
            "time": assignment_sol.execution_time,
        }

        plan.assignment_solutions = assignment_sol
        dump_pickle(self.output()["assignment_sol"].path, assignment_sol)


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
    pick_list_sol = ClsParameter(tpe=AbstractPickListGeneration.return_type())

    def requires(self):
        return {
            "instance": self.instance(),
            "pick_list_sol": self.pick_list_sol(),
        }

    def run(self):
        router: Routing = self._get_inited_router()
        pick_list_sol: BatchingSolution = load_pickle(self.input()["pick_list_sol"]["pick_list_sol"].path)
        pick_lists = pick_list_sol.pick_lists
        routing_sols = []
        for i, pl in enumerate(pick_lists):
            routing_solution = router.solve(pl.pick_positions)
            routing_solution.route.pick_list = pl
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
        routing_solutions = load_pickle(self.input()["routing_sol"]["routing_sol"].path)
        orders = self._load_orders()
        resources = self._load_resources()

        if isinstance(routing_solutions, CombinedRoutingSolution):
            routes = routing_solutions.routes

            sequencing_inpt = SchedulingInput(routes=routes,
                                              orders=orders,
                                              resources=resources)
        else:
            routes = [route.route for route in routing_solutions]
            sequencing_inpt = SchedulingInput(routes=routes,
                                              orders=orders,
                                              resources=resources)
        sequencer = self._get_inited_scheduler()
        sequencing_solution = sequencer.solve(sequencing_inpt)

        dump_pickle(self.output()["scheduling_sol"].path, sequencing_solution)

    def _get_inited_scheduler(self) -> PriorityScheduling:
        ...

    def _load_resources(self) -> Resources:
        return load_pickle(self.input()["instance"]["resources"].path)

    def _load_orders(self) -> OrdersDomain:
        return load_pickle(self.input()["instance"]["orders"].path)

    def _load_routing_data(self):
        return load_pickle(self.input()["routing_input"]["routing_input"].path)

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
            "assignment", "sequencing", "scheduling",
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
    """Distance-only aggregation. Works for both separate and combined paths.

    Only requires the routing terminal — batching, item assignment are
    discovered automatically by walking the graph.
    """
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
    """Aggregation with scheduling and due-date evaluation.

    Only requires the scheduling terminal + instance (for orders).
    Everything else is discovered by walking the graph from scheduling.
    """
    abstract = False
    scheduling_sol = ClsParameter(tpe=AbstractScheduling.return_type())
    instance = ClsParameter(tpe=InstanceLoader.return_type())

    def requires(self):
        return {
            "scheduling_sol": self.scheduling_sol(),
            "instance": self.instance(),
        }

    def _evaluate_due_dates(self, assignments, orders: OrdersDomain):
        order_by_id = {o.order_id: o for o in orders.orders}
        records = []
        for ass in assignments:
            end_time = ass.end_time
            for on in ass.route.pick_list.order_numbers:
                o = order_by_id.get(on)
                if o is None:
                    continue
                if o.due_date is None:
                    continue
                arrival_time = o.order_date
                start_time = ass.start_time
                due_ts = o.due_date
                lateness = end_time - due_ts
                records.append({
                    "order_number": on,
                    "arrival_time": arrival_time,
                    "start_time": start_time,
                    "batch_idx": ass.batch_idx,
                    "picker_id": ass.picker_id,
                    "completion_time": end_time,
                    "due_date": o.due_date,
                    "lateness": lateness,
                    "tardiness": max(0, lateness),
                    "on_time": end_time <= due_ts,
                })
        return pd.DataFrame(records)

    def run(self):
        summary: dict = {}
        collected = self._build_provenance_summary(summary)

        # --- Routing distances (from graph) ---
        routing_entry = collected.get("routing")
        if routing_entry is not None:
            sol = routing_entry["solution"]
            if isinstance(sol, list):
                summary["tours_summary"] = self._compute_tour_summary_from_list(sol)
            elif isinstance(sol, CombinedRoutingSolution):
                summary["tours_summary"] = self._compute_tour_summary_from_combined(sol)

        # --- Scheduling / due-date evaluation ---
        scheduling_entry = collected.get("scheduling")
        if scheduling_entry is None:
            raise ValueError("No scheduling solution found in the task graph.")

        scheduling_sol = scheduling_entry["solution"]
        orders: OrdersDomain = load_pickle(
            self.input()["instance"]["orders"].path
        )

        sequencing_jobs = scheduling_sol.jobs
        due_eval = self._evaluate_due_dates(sequencing_jobs, orders)

        on_time_rate = float(due_eval["on_time"].mean() * 100.0)
        avg_lateness = float(due_eval["lateness"].mean())
        avg_tardiness = float(due_eval["tardiness"].mean())
        max_lateness = float(due_eval["lateness"].max())
        max_tardiness = float(due_eval["tardiness"].max())

        df = pd.DataFrame(sequencing_jobs)
        df["release_dt"] = df["release_time"].apply(lambda x: x)
        df["start_dt"] = df["start_time"].apply(lambda x: x)
        df["end_dt"] = df["end_time"].apply(lambda x: x)

        util = (
            df.groupby("picker_id")[["travel_time", "handling_time"]]
            .sum()
            .assign(total=lambda x: x["travel_time"] + x["handling_time"])
        )

        makespan = df["end_time"].max()
        summary["makespan"] = makespan
        summary["on_time_rate"] = on_time_rate
        summary["avg_lateness"] = avg_lateness
        summary["avg_tardiness"] = avg_tardiness
        summary["max_lateness"] = max_lateness
        summary["max_tardiness"] = max_tardiness

        dump_json(self.output()["summary"].path, summary)



if __name__ == "__main__":
    from luigi.task import flatten

    from pathlib import Path
    from ware_ops_algos.data_loaders import HesslerIrnichLoader

    from ware_ops_pipes.pipelines.components.item_assignment.greedy_item_assignment import GreedyIA
    from ware_ops_pipes.pipelines.components.routing.sprp import RatliffRosenthal
    from ware_ops_pipes.pipelines.components.routing.s_shape import SShape

    import luigi
    from cls.fcl import FiniteCombinatoryLogic
    from cls.subtypes import Subtypes

    from ware_ops_pipes import set_pipeline_params

    from ware_ops_pipes.utils.experiment_utils import PipelineRunner, collect_from_graph


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

    # PROJECT_ROOT = Path(__file__).parent.parent.parent.parent.parent
    # DATA_DIR = PROJECT_ROOT / "data"
    #
    # instances_base = DATA_DIR / "instances"
    # cache_base = DATA_DIR / "instances" / "caches"
    # instance_set = "SPRP"
    # cache_path = cache_base / instance_set
    #
    # instance_name = "unit_F1_m5_C30_a3_0.txt"
    # file_path = instances_base / instance_set / instance_name
    # output_folder = (
    #         PROJECT_ROOT / "experiments" / "output" / "cosy"
    #         / instance_set / instance_name
    # )
    # output_folder.mkdir(parents=True, exist_ok=True)
    #
    # loader = HesslerIrnichLoader(
    #     str(instances_base / instance_set),
    #     str(cache_base / instance_set),
    # )
    # domain = loader.load(str(file_path))
    #
    # output_folder = Path("./output")
    # output_folder.mkdir(parents=True, exist_ok=True)
    # set_pipeline_params(
    #     output_folder=str(output_folder),
    #     instance_set_name="SPRP",
    #     instance_name="instance_name",
    #     instance_path=file_path,
    #     domain_path=loader.cache_path,
    # )
    #
    # target = AbstractResultAggregation.return_type()
    # repository = RepoMeta.repository
    # fcl = FiniteCombinatoryLogic(repository, Subtypes(RepoMeta.subtypes))
    #
    # inhabitation_result = fcl.inhabit(target)
    # max_tasks_when_infinite = 10
    # actual = inhabitation_result.size()
    # max_results = max_tasks_when_infinite
    #
    # if actual is not None or actual == 0:
    #     max_results = actual
    # results = [t() for t in inhabitation_result.evaluated[0:max_results]]
    #
    # if results:
    #     for r in results:
    #         print(print_tree(r))
    #         print("=========================================")
    #
    #     print("Number of results", max_results)
    #     print("Number of results after filtering", len(results))
    #     print("Run Pipelines")
    #     luigi.build(results, local_scheduler=True)
    # else:
    #     print("No results!")

    PROJECT_ROOT = Path(__file__).parent.parent
    DATA_DIR = PROJECT_ROOT / "data"

    instances_base = DATA_DIR / "instances"
    cache_base = DATA_DIR / "instances" / "caches"

    instance_set = "SPRP"  # SPRP-SS

    runner = HesslerIrnichRunner(instance_set, instances_base / instance_set,
                                 cache_base / instance_set, PROJECT_ROOT, verbose=True)

    runner.run_all()
