import copy
import hashlib
import os
import re
from os.path import join as pjoin

import luigi
from cosy_luigi import CoSyLuigiTask, CoSyLuigiTaskParameter

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
from ware_ops_algos.domain_models import OrdersDomain, Resources, LayoutData, Articles, StorageLocations
from ware_ops_algos.domain_models.base_domain import BaseWarehouseDomain
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
        """
        Stable, filesystem-safe task key.

        Uses the Python class identity, not Luigi's task_id.
        The chain fingerprint already separates different upstream/algorithm variants.
        """
        cls = type(self)
        raw = f"{cls.__module__}.{cls.__qualname__}"
        digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()[:10]
        return f"{_safe_path_part(cls.__name__)}_{digest}"

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
        if not domain_path:
            raise ValueError("Pipeline parameter 'domain_path' is not set.")
        domain: BaseWarehouseDomain = load_pickle(domain_path)
        for target in self.output().values():
            os.makedirs(os.path.dirname(target.path), exist_ok=True)
        self.dump_output_pickle("domain", domain)
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

    def _collect(self) -> dict[str, dict]:
        return collect_from_graph(self)

    def _build_provenance_summary(self, summary: dict) -> dict[str, dict]:
        collected = self._collect()
        summary["instance_name"] = self.pipeline_params.instance_name
        summary["instance_set"] = self.pipeline_params.instance_set_name

        provenance_list = []
        for stage_name in ["item_assignment", "batching", "routing", "scheduling"]:
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