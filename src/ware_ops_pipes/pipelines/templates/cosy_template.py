import copy
import hashlib
import os
import re
from os.path import join as pjoin
from typing import Iterable, Mapping, Sequence, Callable

import luigi
from cosy_luigi import CoSyLuigiTask, CoSyLuigiTaskParameter
from ware_ops_algos.algorithms.algorithm_cards import AlgorithmCard

from ware_ops_algos.algorithms.algorithm_interfaces import (
    CombinedRoutingSolution,
    RoutingSolution,
    ItemAssignmentSolution,
    BatchObject,
)
from ware_ops_algos.algorithms import (
    Batching,
    Routing,
    BatchingSolution,
    ItemAssignment,
    RoutingBatchingAssigning,
    build_jobs,
    PriorityScheduler,
)
from ware_ops_algos.domain_algo_mapper.domain_algo_mapper import ConstraintEvaluator
from ware_ops_algos.domain_models import OrdersDomain, Resources, LayoutData, Articles, StorageLocations, DataCard
from ware_ops_algos.domain_models.base_domain import BaseWarehouseDomain
from ware_ops_algos.taxonomy.taxonomy import TAXONOMY
from ware_ops_algos.utils.fingerprint import fingerprint

from ware_ops_pipes.pipelines.io_helpers import dump_pickle, load_pickle, dump_json
from ware_ops_pipes.pipelines.pipeline_params import get_pipeline_params
from ware_ops_pipes.synthesis.pipeline_provenance import collect_from_graph


# class PipelineParams(luigi.Config):
#     output_folder = luigi.Parameter(default=pjoin(os.getcwd(), "outputs"))
#     seed = luigi.IntParameter(default=42)
#
#     time_limit_sec = luigi.OptionalIntParameter(default=240)
#     gen_tour = luigi.BoolParameter(default=False)
#
#     instance_set_name = luigi.Parameter(default=None)
#     instance_name = luigi.Parameter(default=None)
#     instance_path = luigi.Parameter(default=None)
#     domain_path = luigi.Parameter(default=None)


_SAFE_CHARS = re.compile(r"[^A-Za-z0-9_.-]+")


def _safe_path_part(value: str) -> str:
    value = _SAFE_CHARS.sub("_", value)
    return value.strip("._-") or "x"


class BaseComponent(CoSyLuigiTask):
    """
    Base task for CoSy-Luigi benchmark pipelines.

    Important:
    Do not use self.task_id in output paths. With CoSy-Luigi, task_id may include
    generated target information, which can recursively inject absolute paths into
    filenames, especially on Windows.
    """

    algo_cls = None

    def __init__(self, *args, **kwargs):
        self.pipeline_params = copy.copy(get_pipeline_params())
        super().__init__(*args, **kwargs)

    def own_fingerprint(self) -> str:
        """
        Fingerprint of this task's algorithm implementation.

        Concrete algorithm task classes should set:

            algo_cls = ActualAlgorithmClass

        For non-algorithm stages such as InstanceLoader, algo_cls remains None.
        """
        if self.algo_cls is None:
            return ""

        return fingerprint(self.algo_cls)

    def chain_fingerprint(self) -> str:
        """
        Fingerprint of this stage plus all upstream stages.

        If an upstream algorithm changes, all downstream output paths change too.
        """
        req = self.requires()
        deps = req.values() if isinstance(req, dict) else req

        upstream = sorted(
            dep.chain_fingerprint()
            for dep in deps
            if isinstance(dep, BaseComponent)
        )

        payload = "\x00".join([*upstream, self.own_fingerprint()])
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:12]

    def task_path_key(self) -> str:
        name = self.algo_cls.__name__ if self.algo_cls is not None else type(self).__name__
        own_fp = self.own_fingerprint()

        if own_fp:
            return f"{_safe_path_part(name)}@{own_fp[:8]}"

        return _safe_path_part(name)

    def get_luigi_local_target_with_task_id(self, out_name: str) -> luigi.LocalTarget:
        fp = self.chain_fingerprint()
        task_key = self.task_path_key()

        return luigi.LocalTarget(
            pjoin(
                self.pipeline_params.output_folder,
                fp,
                task_key,
                out_name,
            )
        )

    def ensure_output_dirs(self) -> None:
        for target in self.output().values():
            dirname = os.path.dirname(target.path)
            if dirname:
                os.makedirs(dirname, exist_ok=True)

    def dump_output_pickle(self, output_key: str, obj) -> None:
        target = self.output()[output_key]
        dirname = os.path.dirname(target.path)
        if dirname:
            os.makedirs(dirname, exist_ok=True)
        dump_pickle(target.path, obj)

    def dump_output_json(self, output_key: str, obj) -> None:
        target = self.output()[output_key]
        dirname = os.path.dirname(target.path)
        if dirname:
            os.makedirs(dirname, exist_ok=True)
        dump_json(target.path, obj)


# ─────────────────────────── Loading ────────────────────────────────────────

class InstanceLoader(BaseComponent):
    def output(self):
        return {
            # "domain": self.get_luigi_local_target_with_task_id("domain.pkl"),
            "orders": self.get_luigi_local_target_with_task_id("orders.pkl"),
            "resources": self.get_luigi_local_target_with_task_id("resources.pkl"),
            "layout": self.get_luigi_local_target_with_task_id("layout.pkl"),
            "articles": self.get_luigi_local_target_with_task_id("articles.pkl"),
            "storage": self.get_luigi_local_target_with_task_id("storage.pkl"),
            "warehouse_info": self.get_luigi_local_target_with_task_id("warehouse_info.pkl"),
        }

    def run(self):
        domain_path = self.pipeline_params.domain_path
        if not domain_path:
            raise ValueError("Pipeline parameter 'domain_path' is not set.")
        domain: BaseWarehouseDomain = load_pickle(domain_path)
        for target in self.output().values():
            os.makedirs(os.path.dirname(target.path), exist_ok=True)
        # self.dump_output_pickle("domain", domain)
        self.dump_output_pickle("orders", domain.orders)
        self.dump_output_pickle("resources", domain.resources)
        self.dump_output_pickle("layout", domain.layout)
        self.dump_output_pickle("articles", domain.articles)
        self.dump_output_pickle("storage", domain.storage)
        self.dump_output_pickle("warehouse_info", domain.warehouse_info)


# ─────────────────────────── Item Assignment ────────────────────────────────

class AbstractItemAssignment(BaseComponent):
    instance = CoSyLuigiTaskParameter(InstanceLoader)

    def get_inited_item_assigner(self) -> ItemAssignment:
        ...

    def output(self):
        return {
            "item_assignment_sol": self.get_luigi_local_target_with_task_id("item_assignment_sol.pkl")
        }

    def run(self):
        orders_domain = load_pickle(self.input()["instance"]["orders"].path)
        selector = self.get_inited_item_assigner()
        ia_sol = selector.solve(orders_domain.orders)
        orders_domain.orders = ia_sol.resolved_orders
        self.dump_output_pickle("item_assignment_sol", ia_sol)


# ─────────────────────────── Batching ───────────────────────────────────────

class AbstractBatching(BaseComponent):
    instance = CoSyLuigiTaskParameter(InstanceLoader)
    item_assignment_sol = CoSyLuigiTaskParameter(AbstractItemAssignment)

    def output(self):
        return {
            "batching_sol": self.get_luigi_local_target_with_task_id("batching_sol.pkl")
        }


class SingleOrderBatching(AbstractBatching):
    # raw input, no algorithm -> no algo_cls
    def run(self):
        ia_sol: ItemAssignmentSolution = load_pickle(
            self.input()["item_assignment_sol"]["item_assignment_sol"].path
        )
        resolved_orders = ia_sol.resolved_orders

        batches = [BatchObject(batch_id=0, orders=[order]) for order in resolved_orders]

        batching_solution = BatchingSolution(batches=batches)
        batching_solution.execution_time = 0
        batching_solution.algo_name = "RawInput"
        self.dump_output_pickle("batching_sol", batching_solution)


class MultiOrderBatching(AbstractBatching):
    def get_inited_batcher(self) -> Batching:
        ...

    def run(self):
        batcher: Batching = self.get_inited_batcher()
        ia_sol: ItemAssignmentSolution = load_pickle(
            self.input()["item_assignment_sol"]["item_assignment_sol"].path
        )
        resolved_orders = ia_sol.resolved_orders
        batching_sol: BatchingSolution = batcher.solve(resolved_orders)

        if batcher.__class__.__name__ in ["SeedBatching", "ClarkAndWrightBatching", "LocalSearchBatching"]:
            batching_sol.algo_name = batcher.algo_name
        else:
            batching_sol.algo_name = batcher.__class__.__name__

        self.dump_output_pickle("batching_sol", batching_sol)


# ─────────────────────────── Routing ────────────────────────────────────────

class AbstractPickerRouting(BaseComponent):
    instance = CoSyLuigiTaskParameter(InstanceLoader)

    def _get_inited_router(self, start_node: tuple[float, float] | None = None) -> Routing:
        ...

    def output(self):
        return {
            "routing_sol": self.get_luigi_local_target_with_task_id("routing_sol.pkl")
        }

    def _load_resources(self) -> Resources:
        return load_pickle(self.input()["instance"]["resources"].path)

    def _load_storage(self) -> StorageLocations:
        return load_pickle(self.input()["instance"]["storage"].path)

    def _load_layout(self) -> LayoutData:
        return load_pickle(self.input()["instance"]["layout"].path)

    def _load_articles(self) -> Articles:
        return load_pickle(self.input()["instance"]["articles"].path)


class PickerRouting(AbstractPickerRouting):
    batching_sol = CoSyLuigiTaskParameter(AbstractBatching)

    def run(self):
        router: Routing = self._get_inited_router()
        batching_sol: BatchingSolution = load_pickle(
            self.input()["batching_sol"]["batching_sol"].path
        )
        routing_sols = []
        for batch in batching_sol.batches:
            routing_solution = router.solve(batch.pick_positions)
            routing_solution.route.batch = batch
            routing_sols.append(routing_solution)
            router.reset_parameters()
        self.dump_output_pickle("routing_sol", routing_sols)


class CombinedIAR(AbstractPickerRouting):
    # instance only (inherited); no batching upstream
    def run(self):
        orders_domain: OrdersDomain = load_pickle(self.input()["instance"]["orders"].path)
        orders = orders_domain.orders
        router: Routing = self._get_inited_router()
        routing_sols = []
        for order in orders:
            pick_list = [pp for pp in order.order_positions]
            routing_solution = router.solve(pick_list)
            routing_sols.append(routing_solution)
            router.reset_parameters()
        self.dump_output_pickle("routing_sol", routing_sols)


class CombinedBR(AbstractPickerRouting):
    item_assignment_sol = CoSyLuigiTaskParameter(AbstractItemAssignment)

    def _get_inited_router(self, start_node: tuple[float, float] | None = None) -> RoutingBatchingAssigning:
        ...

    def run(self):
        ia_sol: ItemAssignmentSolution = load_pickle(
            self.input()["item_assignment_sol"]["item_assignment_sol"].path
        )
        resolved_orders = ia_sol.resolved_orders
        router = self._get_inited_router()
        combined_solution: CombinedRoutingSolution = router.solve(resolved_orders)
        self.dump_output_pickle("routing_sol", combined_solution)


# ─────────────────────────── Scheduling ─────────────────────────────────────

class AbstractScheduling(BaseComponent):
    instance = CoSyLuigiTaskParameter(InstanceLoader)
    routing_sol = CoSyLuigiTaskParameter(AbstractPickerRouting)

    def output(self):
        return {
            "scheduling_sol": self.get_luigi_local_target_with_task_id("scheduling_sol.pkl")
        }

    def _get_inited_scheduler(self) -> PriorityScheduler:
        ...

    def _load_resources(self) -> Resources:
        return load_pickle(self.input()["instance"]["resources"].path)

    def _load_orders(self) -> OrdersDomain:
        return load_pickle(self.input()["instance"]["orders"].path)

    def run(self):
        routing_solutions = load_pickle(self.input()["routing_sol"]["routing_sol"].path)
        resources = self._load_resources()

        if isinstance(routing_solutions, CombinedRoutingSolution):
            routes = routing_solutions.routes
        else:
            routes = [routing_solution.route for routing_solution in routing_solutions]

        jobs = build_jobs(routes, resources)
        scheduler = self._get_inited_scheduler()
        scheduling_solution = scheduler.solve(jobs)
        scheduling_solution.algo_name = scheduler.__class__.__name__
        self.dump_output_pickle("scheduling_sol", scheduling_solution)


# ─────────────────────────── Result Aggregation ──────────────────────────────

class AbstractResultAggregation(BaseComponent):
    def output(self):
        return {
            "summary": self.get_luigi_local_target_with_task_id("summary.json")
        }

    @classmethod
    def configure(cls, data_card: DataCard, models: list[AlgorithmCard]):
        cls._data_card = data_card
        cls._models = models

    # @classmethod
    # def constraints(cls) -> Sequence[Callable[..., bool]]:
    #     return [
    #         lambda vs: problem_type_constraint(vs, TAXONOMY, cls._data_card, cls._models),
    #         lambda vs: feature_constraint(vs, cls._data_card, cls._models),
    #         lambda vs: check_unique(vs, [AbstractResultAggregation]),
    #     ]

    def _collect(self) -> dict[str, dict]:
        return collect_from_graph(self)

    def _build_provenance_summary(self, summary: dict) -> dict[str, dict]:
        collected = self._collect()

        summary["instance_name"] = self.pipeline_params.instance_name
        summary["instance_set"] = self.pipeline_params.instance_set_name
        summary["pipeline_chain_fingerprint"] = self.chain_fingerprint()

        provenance_list = []

        for stage_name in ["item_assignment", "batching", "routing", "scheduling"]:
            if stage_name in collected:
                entry = collected[stage_name]

                provenance_list.append({
                    "stage": stage_name,
                    "algo": entry["algo"],
                    "time": entry["time"],
                    "task_class": entry["task_class"],
                    "algo_fingerprint": entry.get("algo_fingerprint"),
                    "own_fingerprint": entry.get("own_fingerprint"),
                    "chain_fingerprint": entry.get("chain_fingerprint"),
                    "target_path": entry.get("target_path"),
                })

                summary[f"{stage_name}_algo"] = entry["algo"]
                summary[f"{stage_name}_time"] = entry["time"]
                summary[f"{stage_name}_algo_fingerprint"] = entry.get("algo_fingerprint")
                summary[f"{stage_name}_own_fingerprint"] = entry.get("own_fingerprint")
                summary[f"{stage_name}_chain_fingerprint"] = entry.get("chain_fingerprint")

        summary["provenance"] = provenance_list
        return collected

    @staticmethod
    def _compute_tour_summary_from_list(routing_sols: list[RoutingSolution]) -> dict:
        tour_distances = {}
        routing_times = {}
        total_distance = 0
        for tour_id, sol in enumerate(routing_sols):
            routing_times[f"tour_{tour_id}_time"] = sol.execution_time
            distance = sol.route.distance
            tour_distances[f"tour_{tour_id}_distance"] = distance
            total_distance += distance
        return {"tour_distances": tour_distances, "total_distance": total_distance, "time_per_tour": routing_times}

    @staticmethod
    def _compute_tour_summary_from_combined(combined_sol: CombinedRoutingSolution) -> dict:
        tour_distances = {}
        total_distance = 0
        for tour_id, sol in enumerate(combined_sol.routes):
            distance = sol.distance
            tour_distances[f"tour_{tour_id}_distance"] = distance
            total_distance += distance
        return {"tour_distances": tour_distances, "total_distance": total_distance, "execution_time": combined_sol.execution_time}


class ResultAggregationDistance(AbstractResultAggregation):
    routing_sol = CoSyLuigiTaskParameter(AbstractPickerRouting)

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
        self.dump_output_json("summary", summary)


class ResultAggregationDueDate(AbstractResultAggregation):
    instance = CoSyLuigiTaskParameter(InstanceLoader)
    scheduling_sol = CoSyLuigiTaskParameter(AbstractScheduling)

    def _evaluate_due_dates(self, scheduled_jobs, orders: OrdersDomain):
        import pandas as pd
        order_by_id = {o.order_id: o for o in orders.orders}
        records = []
        for sj in scheduled_jobs:
            job = sj.job
            for order_number in job.order_numbers:
                order = order_by_id.get(order_number)
                if order is None or order.due_date is None:
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
                    "lateness": lateness,
                    "tardiness": max(0.0, lateness),
                    "on_time": sj.end_time <= order.due_date,
                })
        return pd.DataFrame(records)

    @staticmethod
    def _scheduled_jobs_to_frame(scheduled_jobs):
        import pandas as pd
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
            })
        return pd.DataFrame(records)

    def run(self):
        summary: dict = {}
        collected = self._build_provenance_summary(summary)

        routing_entry = collected.get("routing")
        if routing_entry is not None:
            sol = routing_entry["solution"]
            if isinstance(sol, list):
                summary["tours_summary"] = self._compute_tour_summary_from_list(sol)
            elif isinstance(sol, CombinedRoutingSolution):
                summary["tours_summary"] = self._compute_tour_summary_from_combined(sol)

        scheduling_entry = collected.get("scheduling")
        if scheduling_entry is None:
            raise ValueError("No scheduling solution found in the task graph.")

        scheduled_jobs = scheduling_entry["solution"].jobs
        orders: OrdersDomain = load_pickle(self.input()["instance"]["orders"].path)
        due_eval = self._evaluate_due_dates(scheduled_jobs, orders)

        if due_eval.empty:
            for k in ["makespan", "on_time_rate", "avg_lateness", "avg_tardiness", "max_lateness", "max_tardiness"]:
                summary[k] = 0.0
            dump_json(self.output()["summary"].path, summary)
            return

        summary["on_time_rate"] = float(due_eval["on_time"].mean() * 100.0)
        summary["avg_lateness"] = float(due_eval["lateness"].mean())
        summary["avg_tardiness"] = float(due_eval["tardiness"].mean())
        summary["max_lateness"] = float(due_eval["lateness"].max())
        summary["max_tardiness"] = float(due_eval["tardiness"].max())

        df_jobs = self._scheduled_jobs_to_frame(scheduled_jobs)
        summary["makespan"] = float(df_jobs["end_time"].max())
        summary["picker_processing_time"] = {
            str(k): float(v) for k, v in df_jobs.groupby("picker_id")["processing_time"].sum().to_dict().items()
        }
        dump_json(self.output()["summary"].path, summary)


def traverse_pipeline(vs: Iterable[CoSyLuigiTask], visited=None) -> list[CoSyLuigiTask]:
    if visited is None:
        visited = set()

    result = []
    for v in vs:
        vid = id(v)
        if vid in visited:
            continue

        visited.add(vid)
        result.append(v)

        req = v.requires()
        if isinstance(req, dict):
            req = list(req.values())

        result.extend(traverse_pipeline(req, visited))

    return result


def check_unique(
    vs: Mapping[str, CoSyLuigiTask],
    required_to_be_unique: Iterable[type[CoSyLuigiTask]],
    get_classes=None,
) -> bool:
    classes = (
        get_classes(vs)
        if get_classes
        else [pc.__class__ for pc in traverse_pipeline(vs.values())]
    )

    seen_subclasses = {}

    for c in classes:
        for unique in required_to_be_unique:
            if issubclass(c, unique):
                if unique in seen_subclasses and seen_subclasses[unique] != c:
                    return False
                seen_subclasses[unique] = c

    return True


# def problem_type_constraint(vs, subproblems, data_card: DataCard, models, get_classes=None) -> bool:
#     classes = (
#         get_classes(vs)
#         if get_classes
#         else [pc.__class__ for pc in traverse_pipeline(vs.values())]
#     )
#
#     problem = data_card.problem_class
#     problems = subproblems[problem]["variables"]
#
#     for c in classes:
#         for m in models:
#             if m.algo_name == c.__name__:
#                 if m.problem_type not in problems:
#                     print(f"{m.algo_name} not applicable {m.problem_type} not in {problems}")
#                     return False
#
#     return True
#
#
# def feature_constraint(vs, data_card: DataCard, models, get_classes=None) -> bool:
#     classes = (
#         get_classes(vs)
#         if get_classes
#         else [pc.__class__ for pc in traverse_pipeline(vs.values())]
#     )
#
#     domain_sections = {
#         "layout": data_card.layout,
#         "articles": data_card.articles,
#         "orders": data_card.orders,
#         "resources": data_card.resources,
#         "storage": data_card.storage,
#     }
#
#     for c in classes:
#         for m in models:
#             if m.algo_name == c.__name__:
#                 for domain, reqs in m.requirements.items():
#                     section = domain_sections.get(domain)
#
#                     if section is None:
#                         continue
#
#                     required_tpe = reqs["type"]
#                     required_features = reqs.get("features", [])
#                     required_features = [] if required_features in (None, [None]) else required_features
#                     constraints = reqs.get("constraints", {})
#
#                     domain_type = section["type"]
#                     domain_features = [
#                         f for f in section["features"]
#                         if str(section["features"][f]) == "0" or section["features"][f]
#                     ]
#
#                     if "any" not in required_tpe and domain_type not in required_tpe:
#                         print(f"{m.algo_name} not applicable, {domain_type} not in {required_tpe}")
#                         return False
#
#                     missing_features = [f for f in required_features if f not in domain_features]
#                     if missing_features:
#                         print(f"{m.algo_name} not applicable, missing feature: {missing_features}")
#                         return False
#
#                     for feature_name, constraint in constraints.items():
#                         if feature_name not in domain_features:
#                             print(f"{m.algo_name} not applicable, {feature_name} not in {domain_features}")
#                             return False
#
#                         evaluator = ConstraintEvaluator()
#                         if not evaluator.evaluate(feature_name, constraint):
#                             print(f"{m.algo_name} not applicable, {feature_name} not in {domain_features}")
#                             return False
#
#     return True