import os
from os.path import join as pjoin
import pickle
from pathlib import Path
from typing import Sequence, Callable, Iterable, Mapping

import luigi
import ware_ops_algos

from cosy.maestro import Maestro
from luigi import LocalTarget
from luigi.configuration import get_config
from luigi.tools import deps_tree
# from ware_ops_algos.domain_models.taxonomy import SUBPROBLEMS
from ware_ops_algos.utils.general_functions import load_model_cards, ModelCard

from cosy_luigi import CoSyLuigiTask, CoSyLuigiTaskParameter, CoSyLuigiRepo
from ware_ops_algos.data_loaders import HesslerIrnichLoader
from ware_ops_algos.algorithms import (GreedyItemAssignment,
                                       DummyOrderSelection,
                                       SShapeRouting,
                                       FifoBatching,
                                       PlanningState,
                                       OrderNrFifoBatching, Routing, WarehouseOrder, PickList, BatchingSolution)
from ware_ops_algos.domain_models import BaseWarehouseDomain, Articles, Resources, StorageLocations, LayoutData

from ware_ops_pipes import set_pipeline_params
from ware_ops_pipes.utils.io_helpers import load_pickle, dump_pickle

pkg_dir = Path(ware_ops_algos.__file__).parent
model_cards_path = pkg_dir / "algorithms" / "algorithm_cards"
models = load_model_cards(str(model_cards_path))


class PipelineParams(luigi.Config):
    output_folder = luigi.Parameter(default=pjoin(os.getcwd(), "outputs"))
    seed = luigi.IntParameter(default=42)

    instance_set_name = luigi.Parameter(default=None)
    instance_name = luigi.Parameter(default=None)
    instance_path = luigi.Parameter(default=None)
    domain_path = luigi.Parameter(default=None)


class BaseComponent(CoSyLuigiTask):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.pipeline_params = PipelineParams()
        os.makedirs(self.pipeline_params.output_folder, exist_ok=True)

    def get_luigi_local_target_with_task_id(
            self,
            out_name
    ) -> LocalTarget:
        return LocalTarget(
            pjoin(self.pipeline_params.output_folder,
                  self.task_id + "_" + out_name)
        )


########## loading ###############

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

class IA(BaseComponent):
    instance = CoSyLuigiTaskParameter(InstanceLoader)

class GreedyIA(IA):
    def requires(self):
        return {
            "instance": self.instance,
        }

    def output(self):
        return {
            "item_assignment_plan": self.get_luigi_local_target_with_task_id(
                "item_assignment_plan.pkl"
            )
        }

    def run(self):
        orders_domain = load_pickle(self.input()["instance"]["orders"].path)
        storage: StorageLocations = load_pickle(self.input()["instance"]["storage"].path)
        selector = GreedyItemAssignment(storage)
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


########## batching ##############
class Batching(BaseComponent):
    instance = CoSyLuigiTaskParameter(InstanceLoader)
    item_assignment_plan = CoSyLuigiTaskParameter(GreedyIA)

    def requires(self):
        return {
            "instance": self.instance,
            "item_assignment_plan": self.item_assignment_plan,
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

    def output(self):
        return {
            "pick_list_plan": self.get_luigi_local_target_with_task_id(
                "pick_list_plan.pkl"
            )
        }


class RawPickListGeneration(Batching):
    def run(self):
        plan: PlanningState = load_pickle(self.input()["item_assignment_plan"]["item_assignment_plan"].path)
        orders = plan.item_assignment.resolved_orders
        pick_lists = []
        for order in orders:
            pl = self._build_pick_lists([order])
            pick_lists.append(pl)

        batching_solution = BatchingSolution(pick_lists=pick_lists)

        plan.batching_solutions = batching_solution

        plan.provenance["routing_input"] = {
            "algo": "RawInput",
            "time": 0,
        }

        dump_pickle(self.output()["pick_list_plan"].path, plan)


class FiFo(Batching):  # -> BatchedPickListGeneration
    def run(self):
        articles: Articles = load_pickle(self.input()["instance"]["articles"].path)
        resources: Resources = load_pickle(self.input()["instance"]["resources"].path)
        plan: PlanningState = load_pickle(self.input()["item_assignment_plan"]["item_assignment_plan"].path)
        batcher = OrderNrFifoBatching(
            pick_cart=resources.resources[0].pick_cart,
            articles=articles,
        )

        orders = plan.item_assignment.resolved_orders
        pick_lists = []

        for order in orders:
            pl = self._build_pick_lists([order])
            pick_lists.append(pl)
        batching_sol = batcher.solve(orders)

        plan.batching_solutions = batching_sol
        plan.batching_solutions.pick_lists = pick_lists
        plan.provenance["routing_input"] = {
            "algo": "RawInput",
            "time": 0,
        }

        dump_pickle(self.output()["pick_list_plan"].path, plan)


class PickerRouting(BaseComponent):
    instance = CoSyLuigiTaskParameter(InstanceLoader)
    pick_list_plan = CoSyLuigiTaskParameter(Batching)

    def requires(self):
        return {
            "instance": self.instance,
            "pick_list_plan": self.pick_list_plan,
        }

    def _get_inited_router(self) -> Routing:
        pass

    def _load_resources(self) -> Resources:
        return load_pickle(self.input()["instance"]["resources"].path)

    def _load_routing_data(self):
        return load_pickle(self.input()["routing_input"]["routing_input"].path)

    def _load_layout(self) -> LayoutData:
        return load_pickle(self.input()["instance"]["layout"].path)

    def _load_articles(self) -> Articles:
        return load_pickle(self.input()["instance"]["articles"].path)

    def run(self):
        router: Routing = self._get_inited_router()
        plan: PlanningState = load_pickle(self.input()["pick_list_plan"]["pick_list_plan"].path)
        pick_lists = plan.batching_solutions.pick_lists
        for i, pl in enumerate(pick_lists):
            routing_solution = router.solve(pl.pick_positions)
            routing_solution.route.pick_list = pl
            # plan.routing_solutions.append(routing_solution.routes[0])
            plan.routing_solutions.append(routing_solution)

            router.reset_parameters()
        plan.provenance["instance_solving"] = {
            "algo": router.__class__.__name__,
        }

        dump_pickle(self.output()["routing_plan"].path, plan)

    def output(self):
        return {
            "routing_plan": self.get_luigi_local_target_with_task_id(
                "routing_plan.pkl"
            )
        }


class SShape(PickerRouting):
    def _get_inited_router(self):
        resources = self._load_resources()
        layout = self._load_layout()
        layout_network = layout.layout_network
        router = SShapeRouting(
            start_node=layout_network.start_node,
            end_node=layout_network.end_node,
            closest_node_to_start=layout_network.closest_node_to_start,
            min_aisle_position=layout_network.min_aisle_position,
            max_aisle_position=layout_network.max_aisle_position,
            distance_matrix=layout_network.distance_matrix,
            predecessor_matrix=layout_network.predecessor_matrix,
            picker=resources.resources,
            gen_tour=True,
            gen_item_sequence=True,
            node_list=layout_network.node_list,
            node_to_idx={node: idx for idx, node in enumerate(list(layout_network.graph.nodes))},
            idx_to_node={idx: node for idx, node in enumerate(list(layout_network.graph.nodes))}
        )

        return router


class Evaluation(BaseComponent):
    routing_plan = CoSyLuigiTaskParameter(PickerRouting)

    def requires(self):
        return {
            "routing_plan": self.routing_plan,
        }

    def run(self):
        routing_plan = load_pickle(self.input()["routing_plan"]["routing_plan"].path)
        dump_pickle(self.output()["routing_plan"].path, routing_plan)

    def output(self):
        return {
            "routing_plan": self.get_luigi_local_target_with_task_id(
                "routing_plan.pkl"
            )
        }

    @classmethod
    def constraints(cls) -> Sequence[Callable[..., bool]]:
        return [lambda vs: problem_type_constraint(vs, SUBPROBLEMS, WarehouseContext, models)]


SUBPROBLEMS = {"OBRP": {"variables": ["item_assignment", "batching", "routing"]},
               "SPRP": {"variables": ["item_assignment", "routing"]}}


class WarehouseContext:
    problem = "OBRP"
    n_blocks = 1


def traverse_pipeline(vs: Iterable[CoSyLuigiTask]) -> Iterable[CoSyLuigiTask]:
    result = [*vs]
    for v in result:
        # if isinstance(v.requires(), )
        req = v.requires()
        if isinstance(req, dict):
            req = v.requires().values()

        result.extend(traverse_pipeline(req))
    return result


def problem_type_constraint(vs: Mapping[str, CoSyLuigiTask],
                            subproblems: dict,
                            warehouse_context: WarehouseContext,
                            models: list[ModelCard]) -> bool:
    """
        Constrain the applicable components based on the defined problem type in the domain.
        Problem type is retrieved from the InstanceLoader component.
        Valid problem types are defined in the subproblem taxonomy.
        We need to look up the problem type a component fulfills through the model card?
    """
    classes = [pc.__class__ for pc in traverse_pipeline(vs.values())]
    problem = warehouse_context.problem
    problems = subproblems[problem]["variables"]
    for c in classes:
        # print(c.__name__)
        for m in models:
            if m.implementation["class_name"] == c.__name__:
                # print(m.implementation["class_name"])
                # print(m.problem_type)
                if m.problem_type not in problems:
                    # print(m.problem_type)
                    # print(problems)
                    # print("Constraint breached")
                    return False
    return True
    # valid_problem_types = {problem_type} | set(subproblems.get(problem_type, {}).get("variables", []))
    # compatible_algorithms = [algo for algo in algorithms if algo.problem_type in valid_problem_types]


def main():
    PROJECT_ROOT = Path(__file__).parent.parent
    DATA_DIR = PROJECT_ROOT / "data"

    instances_base = DATA_DIR / "instances"
    cache_base = DATA_DIR / "instances" / "caches"
    instance_set = "BahceciOencan"  # SPRP-SS
    cache_path = cache_base / instance_set

    instance_name = "Pr_20_1_20_Store1_01.txt"
    file_path = instances_base / instance_set / instance_name
    output_folder = (
            PROJECT_ROOT / "experiments" / "output" / "cosy"
            / instance_set / instance_name
    )
    output_folder.mkdir(parents=True, exist_ok=True)

    # pkg_dir = Path(ware_ops_algos.__file__).parent
    # model_cards_path = pkg_dir / "algorithms" / "algorithm_cards"
    # models = load_model_cards(str(model_cards_path))

    loader = HesslerIrnichLoader(str(instances_base / instance_set), str(cache_base / instance_set))

    loader.load(str(file_path))

    config = get_config()
    config.set('PipelineParams', 'output_folder', str(output_folder))
    config.set('PipelineParams', 'instance_set_name', instance_set)
    config.set('PipelineParams', 'instance_name', instance_name)
    config.set('PipelineParams', 'instance_path', str(file_path))
    config.set('PipelineParams', 'domain_path', str(loader.cache_path))
    # config.set('PipelineParams', 'models', models)

    repo = CoSyLuigiRepo(InstanceLoader, GreedyIA, RawPickListGeneration, FiFo, SShape, Evaluation)
    maestro = Maestro(repo.cls_repo, repo.taxonomy)
    for result in maestro.query(Evaluation.target()):
        print(deps_tree.print_tree(result))
        # luigi.build([result], local_scheduler=True)

    # results = maestro.query(Evaluation.target())
    # luigi.build([results], local_scheduler=True)


if __name__ == "__main__":
    main()
