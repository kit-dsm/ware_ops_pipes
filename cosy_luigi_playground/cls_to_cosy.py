import os
from os.path import join as pjoin
from pathlib import Path

import luigi
from cosy import Constructor, Maestro, SpecificationBuilder, Var
from luigi import LocalTarget, Task
from luigi.tools import deps_tree

from ware_ops_algos.data_loaders import HesslerIrnichLoader
from ware_ops_algos.algorithms import (GreedyItemAssignment,
                                       DummyOrderSelection,
                                       SShapeRouting,
                                       FifoBatching,
                                       PlanningState,
                                       OrderNrFifoBatching)
from ware_ops_algos.domain_models import BaseWarehouseDomain, Articles, Resources, StorageLocations

from ware_ops_pipes import set_pipeline_params
from ware_ops_pipes.utils.io_helpers import load_pickle, dump_pickle


class PipelineParams(luigi.Config):

    output_folder = luigi.Parameter(default=pjoin(os.getcwd(), "outputs"))
    seed = luigi.IntParameter(default=42)

    instance_set_name = luigi.Parameter(default=None)
    instance_name = luigi.Parameter(default=None)
    instance_path = luigi.Parameter(default=None)
    domain_path = luigi.Parameter(default=None)


class BaseComponent(Task):
    abstract = True

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


class GreedyIA(BaseComponent):
    instance = luigi.TaskParameter(InstanceLoader())


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

class Fifo(BaseComponent): # -> BatchedPickListGeneration
    instance = luigi.TaskParameter(InstanceLoader())
    item_assignment_plan = luigi.TaskParameter(GreedyIA())

    def requires(self):
        return {
            "instance": self.instance,
            "item_assignment_plan": self.item_assignment_plan,
        }

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
        for o in orders:
            pick_list = []
            for pp in o.pick_positions:
                pick_list.append(pp)
            pick_lists.append(pick_list)
        batching_sol = batcher.solve(orders)

        plan.batching_solutions = batching_sol
        plan.provenance["routing_input"] = {
            "algo": "RawInput",
            "time": 0,
        }

        dump_pickle(self.output()["pick_list_plan"].path, plan)

    def output(self):
        return {
            "pick_list_plan": self.get_luigi_local_target_with_task_id(
                "pick_list_plan.pkl"
            )
        }


# Routing
class SShape(luigi.Task): # -> PickerRouting
    
    required_type = luigi.TaskParameter()
    
    def requires(self):
        return self.required_type
    
    def run(self):
        Path("SShape.txt").touch()

    def output(self):
        return luigi.LocalTarget("SShape.txt")

    
########### evaluation ###############

class BaseEvaluator(luigi.Task):
    required_type = luigi.TaskParameter()    
    
    def requires(self):
        return self.required_type
    
    def run(self):
        Path("BaseEvaluator.txt").touch()

    def output(self):
        return luigi.LocalTarget("BaseEvaluator.txt")

    
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

    loader = HesslerIrnichLoader(str(instances_base / instance_set), str(cache_base / instance_set))

    loader.load(str(file_path))

    global_parameters = PipelineParams()
    global_parameters.output_folder = output_folder
    global_parameters.instance_set_name = instance_set
    global_parameters.instance_name = instance_name
    global_parameters.instance_path = str(file_path)
    global_parameters.domain_path = str(loader.cache_path)

    # set_pipeline_params(
    #     output_folder=str(output_folder),
    #     instance_set_name=instance_set,
    #     instance_name=instance_name,
    #     instance_path=str(file_path),
    #     domain_path=str(loader.cache_path)
    # )

    named_components_with_specifications = [

        #### loading ####
        (
            "InstanceLoader",
            lambda: InstanceLoader(),
            SpecificationBuilder()
            .suffix(Constructor("InstanceLoader")),
        ),
        (
            "GreedyIA",
            lambda concrete: GreedyIA(concrete),
            SpecificationBuilder()
            .argument("concrete", Constructor("InstanceLoader"))
            .suffix(Constructor("GreedyIA"))
        ),
        (
            "Fifo",
            lambda concrete: Fifo(concrete),
            SpecificationBuilder()
            .argument("concrete", Constructor("InstanceLoader"))
            .suffix(Constructor("Fifo"))
        )
    ]

    maestro = Maestro(named_components_with_specifications)

    for result in maestro.query(Constructor("Fifo"), 1000):
        # print(deps_tree.print_tree(result))
        luigi.build([result], local_scheduler=True)


if __name__ == "__main__":
    main()
