from pathlib import Path

import luigi
from cosy import Constructor, Maestro, SpecificationBuilder, Var
from luigi.tools import deps_tree


########## loading ###############

class InstanceLoader(luigi.Task):
    
    def run(self):
        Path("InstanceLoader.txt").touch()

    def output(self):
        return luigi.LocalTarget("InstanceLoader.txt")


########## batching ##############

class ClarkAndWrightNN(luigi.Task): # -> BatchedPickListGeneration
    
    required_type = luigi.TaskParameter()
    
    def requires(self):
        return self.required_type
    
    def run(self):
        Path("ClarkAndWrightNN.txt").touch()
    
    def output(self):
        return luigi.LocalTarget("ClarkAndWrightNN.txt")

class ClarkAndWrightRR(luigi.Task): # -> BatchedPickListGeneration
    
    required_type = luigi.TaskParameter()
    
    def requires(self):
        return self.required_type
    
    def run(self):
        Path("ClarkAndWrightRR.txt").touch()

    def output(self):
        return luigi.LocalTarget("ClarkAndWrightRR.txt")
    
class ClarkAndWrightSShape(luigi.Task): # -> BatchedPickListGeneration

    required_type = luigi.TaskParameter()
    
    def requires(self):
        return self.required_type
    
    def run(self):
        Path("ClarkAndWrightSShape.txt").touch()

    def output(self):
        return luigi.LocalTarget("ClarkAndWrightSShape.txt")
    
class DueDate(luigi.Task): # -> BatchedPickListGeneration

    required_type = luigi.TaskParameter()
    
    def requires(self):
        return self.required_type
    
    def run(self):
        Path("DueDate.txt").touch()

    def output(self):
        return luigi.LocalTarget("DueDate.txt")
    
class Fifo(luigi.Task): # -> BatchedPickListGeneration
    
    required_type = luigi.TaskParameter()
    
    def requires(self):
        return self.required_type
    
    def run(self):
        Path("Fifo.txt").touch()

    def output(self):
        return luigi.LocalTarget("Fifo.txt")
    
class LsNnDue(luigi.Task): # -> BatchedPickListGeneration
    
    required_type = luigi.TaskParameter()
    
    def requires(self):
        return self.required_type
    
    def run(self):
        Path("LsNnDue.txt").touch()

    def output(self):
        return luigi.LocalTarget("LsNnDue.txt")
    
class LsNnFifo(luigi.Task): # -> BatchedPickListGeneration
    
    required_type = luigi.TaskParameter()
    
    def requires(self):
        return self.required_type
    
    def run(self):
        Path("LsNnFifo.txt").touch()

    def output(self):
        return luigi.LocalTarget("LsNnFifo.txt")
    
class LsNnRand(luigi.Task): # -> BatchedPickListGeneration
    
    required_type = luigi.TaskParameter()
    
    def requires(self):
        return self.required_type
    
    def run(self):
        Path("LsNnRand.txt").touch()

    def output(self):
        return luigi.LocalTarget("LsNnRand.txt")
    
class LsRr(luigi.Task): # -> BatchedPickListGeneration
    
    required_type = luigi.TaskParameter()
    
    def requires(self):
        return self.required_type
    
    def run(self):
        Path("LsRr.txt").touch()

    def output(self):
        return luigi.LocalTarget("LsRr.txt")
    
class OrderNrFifo(luigi.Task): # -> BatchedPickListGeneration
    
    required_type = luigi.TaskParameter()
    
    def requires(self):
        return self.required_type
    
    def run(self):
        Path("OrderNrFifo.txt").touch()

    def output(self):
        return luigi.LocalTarget("OrderNrFifo.txt")
    
class Random(luigi.Task): # -> BatchedPickListGeneration
    
    required_type = luigi.TaskParameter()
    
    def requires(self):
        return self.required_type
    
    def run(self):
        Path("Random.txt").touch()

    def output(self):
        return luigi.LocalTarget("Random.txt")
    
class SeedSharedArticles(luigi.Task): # -> BatchedPickListGeneration
    
    required_type = luigi.TaskParameter()
    
    def requires(self):
        return self.required_type
    
    def run(self):
        Path("SeedSharedArticles.txt").touch()

    def output(self):
        return luigi.LocalTarget("SeedSharedArticles.txt")
    
class Seed(luigi.Task): # -> BatchedPickListGeneration
    
    required_type = luigi.TaskParameter()
    
    def requires(self):
        return self.required_type
    
    def run(self):
        Path("seed.txt").touch()

    def output(self):
        return luigi.LocalTarget("seed.txt")

########## routing ###############
    
class PickerRouting(luigi.Task): # -> AbstractPickerRouting
    required_type = luigi.TaskParameter()
    
    def requires(self):
        return self.required_type
    
    def run(self):
        Path("PickerRouting.txt").touch()

    def output(self):
        return luigi.LocalTarget("PickerRouting.txt")
    
class CombinedBR(luigi.Task): # -> AbstractPickerRouting
    required_type = luigi.TaskParameter()

    def requires(self):
        return self.required_type
    
    def run(self):
        Path("CombinedBR.txt").touch()

    def output(self):
        return luigi.LocalTarget("CombinedBR.txt")

class ExactSolving(luigi.Task): # -> PickerRouting
    
    required_type = luigi.TaskParameter()
    
    def requires(self):
        return self.required_type
    
    def run(self):
        Path("ExactSolving.txt").touch()

    def output(self):
        return luigi.LocalTarget("ExactSolving.txt")

class LargestGap(luigi.Task): # -> PickerRouting
    
    required_type = luigi.TaskParameter()
    
    def requires(self):
        return self.required_type
    
    def run(self):
        Path("LargestGap.txt").touch()

    def output(self):
        return luigi.LocalTarget("LargestGap.txt")
    
class MidPoint(luigi.Task): # -> PickerRouting
    
    required_type = luigi.TaskParameter()
    
    def requires(self):
        return self.required_type
    
    def run(self):
        Path("MidPoint.txt").touch()

    def output(self):
        return luigi.LocalTarget("MidPoint.txt")

class NearestNeighbor(luigi.Task): # -> PickerRouting
    
    required_type = luigi.TaskParameter()
    
    def requires(self):
        return self.required_type
    
    def run(self):
        Path("NearestNeighbor.txt").touch()

    def output(self):
        return luigi.LocalTarget("NearestNeighbor.txt")
    
class Return(luigi.Task): # -> PickerRouting
    
    required_type = luigi.TaskParameter()
    
    def requires(self):
        return self.required_type
    
    def run(self):
        Path("return.txt").touch()

    def output(self):
        return luigi.LocalTarget("return.txt")

class SShape(luigi.Task): # -> PickerRouting
    
    required_type = luigi.TaskParameter()
    
    def requires(self):
        return self.required_type
    
    def run(self):
        Path("SShape.txt").touch()

    def output(self):
        return luigi.LocalTarget("SShape.txt")
    
class RatliffRosenthal(luigi.Task): # -> PickerRouting
    
    required_type = luigi.TaskParameter()
    
    def requires(self):
        return self.required_type
    
    def run(self):
        Path("RatliffRosenthal.txt").touch()

    def output(self):
        return luigi.LocalTarget("RatliffRosenthal.txt")

class CombinedBatchingRoutingAssignment(luigi.Task): # -> CombinedBR
    
    required_type = luigi.TaskParameter()
    
    def requires(self):
        return self.required_type
    
    def run(self):
        Path("CombinedBatchingRoutingAssignment.txt").touch()

    def output(self):
        return luigi.LocalTarget("CombinedBatchingRoutingAssignment.txt")

class RoutingTargetNode(luigi.Task):
    required_type = luigi.TaskParameter()    
    
    def requires(self):
        return self.required_type
    
    def run(self):
        Path("RoutingTargetNode.txt").touch()

    def output(self):
        return luigi.LocalTarget("RoutingTargetNode.txt")    
    
########### evaluation ###############

class BaseEvaluator(luigi.Task):
    required_type = luigi.TaskParameter()    
    
    def requires(self):
        return self.required_type
    
    def run(self):
        Path("BaseEvaluator.txt").touch()

    def output(self):
        return luigi.LocalTarget("BaseEvaluator.txt")
    
class SomeOtherEvaluator(luigi.Task):
    required_type = luigi.TaskParameter()    
    
    def requires(self):
        return self.required_type
    
    def run(self):
        Path("SomeOtherEvaluator.txt").touch()

    def output(self):
        return luigi.LocalTarget("SomeOtherEvaluator.txt")
    
def main():
    
    named_components_with_specifications = [

        #### loading ####
        (
            "InstanceLoader",
            lambda: InstanceLoader(),
            SpecificationBuilder()
            .suffix(Constructor("InstanceLoader")),
        ),
        
        #### batching ####
        (
            "ClarkAndWrightNN",
            lambda concrete: ClarkAndWrightNN(concrete),
            SpecificationBuilder()
            .argument("concrete", Constructor("InstanceLoader"))
            .suffix(Constructor("BatchedPickListGeneration"))
        ),
        (
            "ClarkAndWrightRR",
            lambda concrete: ClarkAndWrightRR(concrete),
            SpecificationBuilder()
            .argument("concrete", Constructor("InstanceLoader"))
            .suffix(Constructor("BatchedPickListGeneration"))
        ),
        (
            "ClarkAndWrightSShape",
            lambda concrete: ClarkAndWrightSShape(concrete),
            SpecificationBuilder()
            .argument("concrete", Constructor("InstanceLoader"))
            .suffix(Constructor("BatchedPickListGeneration"))
        ),
        (
            "DueDate",
            lambda concrete: DueDate(concrete),
            SpecificationBuilder()
            .argument("concrete", Constructor("InstanceLoader"))
            .suffix(Constructor("BatchedPickListGeneration"))
        ),
        (
            "Fifo",
            lambda concrete: Fifo(concrete),
            SpecificationBuilder()
            .argument("concrete", Constructor("InstanceLoader"))
            .suffix(Constructor("BatchedPickListGeneration"))
        ),
        (
            "LsNnDue",
            lambda concrete: LsNnDue(concrete),
            SpecificationBuilder()
            .argument("concrete", Constructor("InstanceLoader"))
            .suffix(Constructor("BatchedPickListGeneration"))
        ),
        (
            "LsNnFifo",
            lambda concrete: LsNnFifo(concrete),
            SpecificationBuilder()
            .argument("concrete", Constructor("InstanceLoader"))
            .suffix(Constructor("BatchedPickListGeneration"))
        ),
        (
            "LsNnRand",
            lambda concrete: LsNnRand(concrete),
            SpecificationBuilder()
            .argument("concrete", Constructor("InstanceLoader"))
            .suffix(Constructor("BatchedPickListGeneration"))
        ),
        (
            "LsRr",
            lambda concrete: LsRr(concrete),
            SpecificationBuilder()
            .argument("concrete", Constructor("InstanceLoader"))
            .suffix(Constructor("BatchedPickListGeneration"))
        ),
        (
            "OrderNrFifo",
            lambda concrete: OrderNrFifo(concrete),
            SpecificationBuilder()
            .argument("concrete", Constructor("InstanceLoader"))
            .suffix(Constructor("BatchedPickListGeneration"))
        ),
        (
            "Random",
            lambda concrete: Random(concrete),
            SpecificationBuilder()
            .argument("concrete", Constructor("InstanceLoader"))
            .suffix(Constructor("BatchedPickListGeneration"))
        ),
        (
            "SeedSharedArticles",
            lambda concrete: SeedSharedArticles(concrete),
            SpecificationBuilder()
            .argument("concrete", Constructor("InstanceLoader"))
            .suffix(Constructor("BatchedPickListGeneration"))
        ),
        (
            "Seed",
            lambda concrete: Seed(concrete),
            SpecificationBuilder()
            .argument("concrete", Constructor("InstanceLoader"))
            .suffix(Constructor("BatchedPickListGeneration")),
        ),
        
        
        #### routing ####
        
            ### picker routing ###
        (
            "ExactSolving",
            lambda concrete: ExactSolving(concrete),
            SpecificationBuilder()
            .argument("concrete", Constructor("BatchedPickListGeneration"))
            .suffix(Constructor("PickerRouting")),
        ),
        (
            "LargestGap",
            lambda concrete: LargestGap(concrete),
            SpecificationBuilder()
            .argument("concrete", Constructor("BatchedPickListGeneration"))
            .suffix(Constructor("PickerRouting")),
        ),
        (
            "MidPoint",
            lambda concrete: MidPoint(concrete),
            SpecificationBuilder()
            .argument("concrete", Constructor("BatchedPickListGeneration"))
            .suffix(Constructor("PickerRouting")),
        ),        
        (
            "NearestNeighbor",
            lambda concrete: NearestNeighbor(concrete),
            SpecificationBuilder()
            .argument("concrete", Constructor("BatchedPickListGeneration"))
            .suffix(Constructor("PickerRouting")),
        ),
        (
            "Return",
            lambda concrete: Return(concrete),
            SpecificationBuilder()
            .argument("concrete", Constructor("BatchedPickListGeneration"))
            .suffix(Constructor("PickerRouting")),
        ),
        (
            "SShape",
            lambda concrete: SShape(concrete),
            SpecificationBuilder()
            .argument("concrete", Constructor("BatchedPickListGeneration"))
            .suffix(Constructor("PickerRouting")),
        ),
        (
            "RatliffRosenthal",
            lambda concrete: RatliffRosenthal(concrete),
            SpecificationBuilder()
            .argument("concrete", Constructor("BatchedPickListGeneration"))
            .suffix(Constructor("PickerRouting")),
        ),

            ### combined batching routing ###
        (
            "CombinedBatchingRoutingAssignment",
            lambda concrete: CombinedBatchingRoutingAssignment(concrete),
            SpecificationBuilder()
            .argument("concrete", Constructor("BatchedPickListGeneration"))
            .suffix(Constructor("CombinedBR")),
        ),
        
            ### target nodes ###
            
            # are used to make CombinedBR and PickerRouting subclasses of AbstractPickerRouting, to pass to evaluators as a single class
            # TODO possible to not add target node classes, but use logical OR?
        (
            "CombinedBRTarget",
            lambda concrete: RoutingTargetNode(concrete),
            SpecificationBuilder()
            .argument("concrete", Constructor("CombinedBR"))
            .suffix(Constructor("AbstractPickerRouting")),
        ),
        (
            "PickerRoutingTarget",
            lambda concrete: RoutingTargetNode(concrete),
            SpecificationBuilder()
            .argument("concrete", Constructor("PickerRouting"))
            .suffix(Constructor("AbstractPickerRouting")),
        ),
        
        #### evaluation ####
        (
            "BaseEvaluator",
            lambda concrete: BaseEvaluator(concrete),
            SpecificationBuilder()
            .argument("concrete", Constructor("AbstractPickerRouting"))
            .suffix(Constructor("Evaluator")),
        ),
        (
            "SomeOtherEvaluator",
            lambda concrete: SomeOtherEvaluator(concrete),
            SpecificationBuilder()
            .argument("concrete", Constructor("AbstractPickerRouting"))
            .suffix(Constructor("Evaluator")),
        ),
    ]
    
    maestro = Maestro(named_components_with_specifications)
    
    for result in maestro.query(Constructor("Evaluator"), 1000):
        print(deps_tree.print_tree(result))
        luigi.build([result], local_scheduler=True)


if __name__ == "__main__":
    main()
