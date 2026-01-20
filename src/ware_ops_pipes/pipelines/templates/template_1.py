import os

import pandas as pd
from cls_luigi.inhabitation_task import ClsParameter

from ware_ops_algos.algorithms import  Batching, Routing, Route, \
    BatchingSolution, ItemAssignment, RoutingBatchingAssigning, Assigner
from ware_ops_algos.algorithms.algorithm import PlanningState, CombinedRoutingSolution, RoutingSolution, BatchObject, \
    WarehouseOrder, PickList
from ware_ops_algos.algorithms.order_selection import OrderSelection
from ware_ops_algos.algorithms.scheduling.scheduling import PriorityScheduling, SchedulingInput, PickListSequencer
from ware_ops_algos.domain_models import OrdersDomain, Resources, LayoutData, Articles
from ware_ops_algos.domain_models.base_domain import BaseWarehouseDomain
from ware_ops_pipes.pipelines import BaseComponent
from ware_ops_pipes.utils.io_helpers import dump_pickle, load_pickle, load_json, dump_json


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
            "item_assignment_plan": self.get_luigi_local_target_with_task_id(
                "item_assignment_plan.pkl"
            )
        }

    def run(self):
        orders_domain = load_pickle(self.input()["instance"]["orders"].path)
        selector = self.get_inited_item_assigner()

        # orders["batch_number"] = orders["order_number"]
        # storage_locations = load_pickle(self.input()["instance"]["storage"].path)
        # selector = GreedyPickLocationSelector(orders.orders, storage_locations)
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
        dump_pickle(self.output()["item_assignment_plan"].path, plan)


class AbstractOrderSelection(BaseComponent):
    abstract = True
    instance = ClsParameter(tpe=InstanceLoader.return_type())
    item_assignment_plan = ClsParameter(tpe=AbstractItemAssignment.return_type())

    def get_inited_order_selector(self) -> OrderSelection:
        ...

    def requires(self):
        return {
            "instance": self.instance(),
            "item_assignment_plan": self.item_assignment_plan()
        }

    def output(self):
        return {
            "order_selection_plan": self.get_luigi_local_target_with_task_id(
                "order_selection_plan.pkl"
            )
        }

    def run(self):
        plan: PlanningState = load_pickle(self.input()["item_assignment_plan"]["item_assignment_plan"].path)
        resolved_orders = plan.item_assignment.resolved_orders
        order_selector = self.get_inited_order_selector()
        sol = order_selector.solve(resolved_orders)
        # sol = OrderSelectionSolution(selected_orders=resolved_orders)

        plan.order_selection_solutions = sol

        plan.provenance["order_selection"] = {
            "algo": order_selector.__class__.__name__,
            "time": sol.execution_time,
        }
        dump_pickle(self.output()["order_selection_plan"].path, plan)



class AbstractPickListGeneration(BaseComponent):
    abstract = True
    instance = ClsParameter(tpe=InstanceLoader.return_type())
    # item_assignment_plan = ClsParameter(tpe=AbstractItemAssignment.return_type())
    order_selection_plan = ClsParameter(tpe=AbstractOrderSelection.return_type())

    def requires(self):
        return {
            "instance": self.instance(),
            # "resolved_orders": self.resolved_orders()
            "order_selection_plan": self.order_selection_plan()
        }

    def output(self):
        return {
            # "routing_input": self.get_luigi_local_target_with_task_id(
            #     "routing_input.pkl"
            # ),
            "pick_list_plan": self.get_luigi_local_target_with_task_id(
                "pick_list_plan.pkl"
            )
        }

    @staticmethod
    def _latest_order_arrival(orders: list[WarehouseOrder]) -> float:
        if any(o.order_date is not None for o in orders):
            arrivals = [o.order_date for o in orders]
            return max(arrivals) if arrivals else 0.0
        else:
            return 0.0

    @staticmethod
    def _first_due_date(orders: list[WarehouseOrder]) -> float:
        if any(o.order_date is not None for o in orders):
            due_dates = [o.order_date for o in orders]
            return min(due_dates) if due_dates else float("inf")
        else:
            return 0.0

    def _build_pick_lists(self, orders: list[WarehouseOrder]):
        # build pick lists
        pick_positions = []
        for order in orders:
            for pos in order.pick_positions:
                pick_positions.append(pos)

        pick_list = PickList(
            pick_positions=pick_positions,
            release=self._latest_order_arrival(orders),
            earliest_due_date=self._first_due_date(orders),
            orders=orders
        )
        return pick_list


class RawPickListGeneration(AbstractPickListGeneration):
    abstract = False

    def run(self):
        plan: PlanningState = load_pickle(self.input()["order_selection_plan"]["order_selection_plan"].path)
        resolved_orders = plan.order_selection_solutions.selected_orders
        pick_lists = []
        for order in resolved_orders:
            pl = self._build_pick_lists([order])
            pick_lists.append(pl)

        batching_solution = BatchingSolution(pick_lists=pick_lists)

        plan.batching_solutions = batching_solution

        plan.provenance["routing_input"] = {
            "algo": "RawInput",
            "time": 0,
        }

        dump_pickle(self.output()["pick_list_plan"].path, plan)


class BatchedPickListGeneration(AbstractPickListGeneration):
    abstract = True

    def get_inited_batcher(self) -> Batching:
        ...

    def run(self):
        batcher: Batching = self.get_inited_batcher()
        plan: PlanningState = load_pickle(self.input()["order_selection_plan"]["order_selection_plan"].path)
        resolved_orders = plan.order_selection_solutions.selected_orders

        batching_sol = batcher.solve(resolved_orders)

        batches = batching_sol.batches
        pick_lists = []
        for batch in batches:
            pl = self._build_pick_lists(batch.orders)
            pick_lists.append(pl)

        batching_sol.pick_lists = pick_lists

        plan.batching_solutions = batching_sol

        if batcher.__class__.__name__ in ["SeedBatching", "ClarkAndWrightBatching", "LocalSearchBatching"]:
            batching_algo_name = batcher.algo_name
        else:
            batching_algo_name = batcher.__class__.__name__

        plan.provenance["routing_input"] = {
            "algo": batching_algo_name,
            "time": batcher.execution_time,
        }

        dump_pickle(self.output()["pick_list_plan"].path, plan)


class AbstractSequencing(BaseComponent):
    abstract = True
    instance = ClsParameter(tpe=InstanceLoader.return_type())
    pick_list_plan = ClsParameter(tpe=AbstractPickListGeneration.return_type())

    def requires(self):
        return {
            "instance": self.instance(),
            "pick_list_plan": self.pick_list_plan()
        }

    def output(self):
        return {
            "sequencing_plan": self.get_luigi_local_target_with_task_id(
                "sequencing_plan.pkl")
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
            "pick_list_plan": self.pick_list_plan()
        }

    def run(self):
        sequencer = self._get_inited_sequencer()
        plan: PlanningState = load_pickle(self.input()["pick_list_plan"]["pick_list_plan"].path)
        pick_lists = plan.batching_solutions.pick_lists
        sequencing_sol = sequencer.solve(pick_lists)

        plan.sequencing_solutions = sequencing_sol
        plan.provenance["sequencing"] = {
            "algo": sequencer.__class__.__name__,
            "time": sequencing_sol.execution_time,
        }

        dump_pickle(self.output()["sequencing_plan"].path, plan)


class AbstractPickerAssignment(BaseComponent):
    abstract = True
    instance = ClsParameter(tpe=InstanceLoader.return_type())
    pick_list_plan = ClsParameter(tpe=AbstractPickListGeneration.return_type())

    def requires(self):
        return {
            "instance": self.instance(),
            "pick_list_plan": self.pick_list_plan()
        }

    def output(self):
        return {
            "assignment_plan": self.get_luigi_local_target_with_task_id(
                "assignment_plan.pkl")
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
        dump_pickle(self.output()["assignment_plan"].path, plan)


class AbstractPickerRouting(BaseComponent):
    abstract = True
    instance = ClsParameter(tpe=InstanceLoader.return_type())

    def requires(self):
        return {
            "instance": self.instance(),
        }

    def output(self):
        return {
            "routing_plan": self.get_luigi_local_target_with_task_id(
                "routing_plan.pkl")
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
    pick_list_plan = ClsParameter(tpe=AbstractPickListGeneration.return_type())

    def requires(self):
        return {
            "instance": self.instance(),
            "pick_list_plan": self.pick_list_plan(),
        }

    def run(self):
        router: Routing = self._get_inited_router()
        plan: PlanningState = load_pickle(self.input()["pick_list_plan"]["pick_list_plan"].path)
        pick_lists = plan.batching_solutions.pick_lists
        for i, pl in enumerate(pick_lists):
            # picker = assignment.picker
            # TODO Now we could use current picker pos as start node?
            routing_solution = router.solve(pl.pick_positions)
            routing_solution.route.pick_list = pl
            # plan.routing_solutions.append(routing_solution.routes[0])
            plan.routing_solutions.append(routing_solution)

            router.reset_parameters()
        plan.provenance["instance_solving"] = {
            "algo": router.__class__.__name__,
        }

        dump_pickle(self.output()["routing_plan"].path, plan)


class CombinedBR(AbstractPickerRouting):
    abstract = True
    item_assignment_plan = ClsParameter(tpe=AbstractItemAssignment.return_type())

    def requires(self):
        return {
            "instance": self.instance(),
            "item_assignment_plan": self.item_assignment_plan()
        }

    def run(self):
        # resolved_orders = load_pickle(self.input()["resolved_orders"]["resolved_orders"].path)
        plan: PlanningState = load_pickle(self.input()["item_assignment_plan"]["item_assignment_plan"].path)
        resolved_orders = plan.item_assignment.resolved_orders
        pick_list = []
        for order in resolved_orders:
            for pos in order.order_positions:
                pick_list.append(pos)

        router = self._get_inited_router()
        combined_solution: CombinedRoutingSolution = router.solve(pick_list)
        batches = []
        for i, r in enumerate(combined_solution.routes):
            pick_positions = []
            batched_orders = []

            for onr in r.order_numbers:
                for o in resolved_orders:
                    if o.order_id == onr:
                        batched_orders.append(o)
                        pick_positions += o.order_positions
            r.pick_positions = pick_positions
            route = Route(route=r.route,
                          item_sequence=r.item_sequence,
                          distance=r.distance,
                          )
            batch = BatchObject(
                batch_id=i,
                orders=batched_orders)
            batches.append(batch)
            routing_solution = RoutingSolution(route=route)
            plan.routing_solutions.append(routing_solution)

        plan.provenance["routing_input"] = {
            "algo": router.__class__.__name__,
            "time": 0,
        }

        plan.batching_solutions = BatchingSolution(execution_time=0, batches=batches)

        plan.provenance["instance_solving"] = {
            "algo": router.__class__.__name__,
            "time": router.execution_time,
        }
        # dump_pickle(self.output()["routing_input_plan"].path, plan)
        dump_pickle(self.output()["routing_plan"].path, plan)

    def _get_inited_router(self, start_node: tuple[float, float] | None = None) -> RoutingBatchingAssigning:
        ...


class AbstractScheduling(BaseComponent):
    abstract = True
    routing_plan = ClsParameter(tpe=AbstractPickerRouting.return_type())
    instance = ClsParameter(tpe=InstanceLoader.return_type())

    def requires(self):
        return {
            "routing_plan": self.routing_plan(),
            "instance": self.instance(),
        }

    def output(self):
        return {
            "scheduling_plan": self.get_luigi_local_target_with_task_id(
                "scheduling_plan.pkl"
            )
        }

    def run(self):
        # routing_solution = self._load_routing_solution()
        plan: PlanningState = load_pickle(self.input()["routing_plan"]["routing_plan"].path)
        routing_solutions = plan.routing_solutions
        routes = []
        for route in routing_solutions:
            routes.append(route.route)
        orders = self._load_orders()
        resources = self._load_resources()
        sequencing_inpt = SchedulingInput(routes=routes,
                                          orders=orders,
                                          resources=resources)
        sequencer = self._get_inited_sequencer()
        sequencing_solution = sequencer.solve(sequencing_inpt)

        plan.sequencing_solutions = sequencing_solution
        plan.provenance["sequencing"] = {
            "algo": sequencer.__class__.__name__,
        }

        dump_pickle(self.output()["scheduling_plan"].path, plan)

    def _get_inited_sequencer(self) -> PriorityScheduling:
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
    routing_plan = ClsParameter(tpe=AbstractPickerRouting.return_type())

    def requires(self):
        return {
            "routing_plan": self.routing_plan()
        }

    def output(self):
        return {
            "summary": self.get_luigi_local_target_with_task_id(
                "summary.json"
            )
        }

    def _build_base_summary(self, summary: dict):
        plan: PlanningState = load_pickle(self.input()["routing_plan"]["routing_plan"].path)
        item_assignment_name = plan.provenance["item_assignment"]["algo"]
        routing_algo_name = plan.provenance["instance_solving"]["algo"]
        batching_algo_name = plan.provenance["routing_input"]["algo"]
        summary["item_assignment_algo"] = item_assignment_name
        summary["routing_algo"] = routing_algo_name
        summary["batching_algo"] = batching_algo_name
        summary["instance_name"] = self.pipeline_params.instance_name
        summary["instance_set"] = self.pipeline_params.instance_set_name
        return summary


class ResultAggregationDistance(AbstractResultAggregation):
    abstract = False
    routing_plan = ClsParameter(tpe=AbstractPickerRouting.return_type())

    def requires(self):
        return {
            "routing_plan": self.routing_plan()
        }

    def run(self):
        summary = {}
        summary = self._build_base_summary(summary)
        plan: PlanningState = load_pickle(self.input()["routing_plan"]["routing_plan"].path)

        summary["tours_summary"] = {}

        routing_input_time = plan.batching_solutions.execution_time

        tour_distances = {}
        routing_times = {}

        total_distance = 0
        routing_sols = plan.routing_solutions
        tour_id = 0
        tours = []

        for sol in routing_sols:
            tours.append(sol.route)
            routing_times[f"tour_{tour_id}_time"] = sol.execution_time

        for tour in tours:
            distance = tour.distance
            tour_id += 1
            tour_distances[f"tour_{tour_id}_distance"] = distance
            total_distance += distance
        summary["tours_summary"]["tour_distances"] = tour_distances
        summary["tours_summary"]["total_distance"] = total_distance
        summary["tours_summary"]["routing_input_time"] = routing_input_time
        summary["tours_summary"]["time_per_tour"] = routing_times

        dump_json(self.output()["summary"].path, summary)

    def output(self):
        return {
            "summary": self.get_luigi_local_target_with_task_id(
                "summary.json"
            )
        }


class ResultAggregationMakespan(AbstractResultAggregation):
    abstract = False
    routing_plan = ClsParameter(tpe=AbstractPickerRouting.return_type())
    scheduling_plan = ClsParameter(tpe=AbstractScheduling.return_type())

    def requires(self):
        return {
            "routing_plan": self.routing_plan(),
            "scheduling_plan": self.scheduling_plan()
        }

    def output(self):
        return {
            "summary": self.get_luigi_local_target_with_task_id(
                "summary.json"
            )
        }

    def run(self):
        summary = {}
        summary = self._build_base_summary(summary)
        # scheduling_algo_name = self.requires()["scheduling_plan"].__class__.__name__
        plan: PlanningState = load_pickle(self.input()["scheduling_plan"]["scheduling_plan"].path)
        sequencing_algo_name = plan.provenance["sequencing"]["algo"]
        summary["scheduling_algo"] = sequencing_algo_name

        scheduling_data = load_pickle(self.input()["scheduling_plan"]["scheduling_plan"].path)
        sequencing = plan.sequencing_solutions.jobs

        df = pd.DataFrame(sequencing)
        df["release_dt"] = df["release_time"].apply(lambda x: x)
        df["start_dt"] = df["start_time"].apply(lambda x: x)
        df["end_dt"] = df["end_time"].apply(lambda x: x)

        util = (
            df.groupby("picker_id")[["travel_time", "handling_time"]]
            .sum()
            .assign(total=lambda x: x["travel_time"] + x["handling_time"])
        )
        print("\nUtilization summary (busy time by picker):")
        print(util.to_string())

        makespan = df["end_time"].max()
        summary["makespan"] = makespan

        dump_json(self.output()["summary"].path, summary)


class ResultAggregationDueDate(AbstractResultAggregation):
    abstract = False
    routing_plan = ClsParameter(tpe=AbstractPickerRouting.return_type())
    scheduling_plan = ClsParameter(tpe=AbstractScheduling.return_type())
    instance = ClsParameter(tpe=InstanceLoader.return_type())

    def requires(self):
        return {
            "routing_plan": self.routing_plan(),
            "scheduling_plan": self.scheduling_plan(),
            "instance": self.instance()
        }

    def output(self):
        return {
            "summary": self.get_luigi_local_target_with_task_id(
                "summary.json"
            )
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
                    continue  # skip if no due date
                arrival_time = o.order_date
                start_time = ass.start_time
                due_ts = o.due_date  # .timestamp()
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
        summary = {}
        summary = self._build_base_summary(summary)
        # scheduling_algo_name = self.requires()["scheduling_plan"].__class__.__name__
        # summary["scheduling_algo"] = scheduling_algo_name

        plan: PlanningState = load_pickle(self.input()["scheduling_plan"]["scheduling_plan"].path)
        sequencing_algo_name = plan.provenance["sequencing"]["algo"]
        summary["scheduling_algo"] = sequencing_algo_name

        # scheduling_data = load_pickle(self.input()["scheduling_plan"]["scheduling_plan"].path)
        orders = load_pickle(self.input()["instance"]["orders"].path)
        # schedules = scheduling_data["scheduling_plan"].assignments
        sequencing = plan.sequencing_solutions.jobs
        due_eval = self._evaluate_due_dates(sequencing, orders)

        on_time_rate = float(due_eval["on_time"].mean() * 100.0)
        avg_lateness = float(due_eval["lateness"].mean())
        avg_tardiness = float(due_eval["tardiness"].mean())
        max_lateness = float(due_eval["lateness"].max())
        max_tardiness = float(due_eval["tardiness"].max())

        summary["on_time_rate"] = on_time_rate
        summary["avg_lateness"] = avg_lateness
        summary["avg_tardiness"] = avg_tardiness
        summary["max_lateness"] = max_lateness
        summary["max_tardiness"] = max_tardiness
        print(summary)
        dump_json(self.output()["summary"].path, summary)
