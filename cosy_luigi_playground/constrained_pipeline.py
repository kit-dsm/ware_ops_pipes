from pathlib import Path

import luigi
from cosy import Constructor, Maestro, SpecificationBuilder
from luigi.tools import deps_tree


########## loading ###############

class InstanceLoader(luigi.Task):

    def run(self):
        Path("InstanceLoader.txt").touch()

    def output(self):
        return luigi.LocalTarget("InstanceLoader.txt")


########## batching ##############

class Batching(luigi.Task):

    required_type = luigi.TaskParameter() # Constructor("InstanceLoader")
    constrained = luigi.BoolParameter() # True | False

    def requires(self):
        return self.required_type

    def run(self):
        Path(f"Batching_{self.constrained}.txt").touch()

    def output(self):
        return luigi.LocalTarget(f"Batching_{self.constrained}.txt")


########## routing ###############

class Routing(luigi.Task):

    required_type = luigi.TaskParameter() # Constructor("Batching")
    routing_type = luigi.Parameter() # "type1" | "type2"
    constrained = luigi.BoolParameter() # True | False

    def requires(self):
        return self.required_type

    def run(self):
        Path(
            f"Routing_{self.routing_type}_{self.constrained}.txt"
        ).touch()

    def output(self):
        return luigi.LocalTarget(
            f"Routing_{self.routing_type}_{self.constrained}.txt"
        )


########## evaluation ############

class Evaluator(luigi.Task):
    routing = luigi.TaskParameter() # Constructor("Routing")
    
    def requires(self):
        return self.routing

    def run(self):
        Path("Evaluator.txt").touch()

    def output(self):
        return luigi.LocalTarget("Evaluator.txt")


########## cosy parameters ##########

class Bool(luigi.Parameter):
    name = "constrained"

    def __iter__(self):  # type: ignore
        # yield from frozenset((True, False))
        yield True
        yield False


class RoutingType(luigi.Parameter):
    name = "routing_type"

    def __iter__(self):  # type: ignore
        # yield from frozenset(("type1", "type2"))
        yield "type1"
        yield "type2"

# routing_constraint_a == "type2" => routing_constraint_b == True
def routing_constraint(vs: dict[str, any]) -> bool:
    return (vs["routing_constraint_a"] == "type2" and vs["routing_constraint_b"]) or (vs["routing_constraint_a"] == "type1")

########## cosy wiring ###########

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
            "Batching",
            lambda concrete, constrained:
                Batching(concrete, constrained),
            SpecificationBuilder()
                .argument("concrete", Constructor("InstanceLoader"))
                .parameter("constrained", Bool())
                .suffix(Constructor("Batching")),
        ),

        #### routing ####
        (
            "Routing",
            lambda concrete, routing_constraint_a, routing_constraint_b:
                Routing(concrete, routing_constraint_a, routing_constraint_b),
            SpecificationBuilder()
                .argument("concrete", Constructor("Batching"))
                .parameter("routing_constraint_a", RoutingType())
                .parameter("routing_constraint_b", Bool())
                .constraint(lambda vs: routing_constraint(vs))
                .suffix(Constructor("Routing")),
        ),

        #### evaluation ####
        (
            "Evaluator",
            lambda concrete: Evaluator(concrete),
            SpecificationBuilder()
                .argument("concrete", Constructor("Routing"))
                .suffix(Constructor("Evaluator")),
        ),
    ]

    maestro = Maestro(named_components_with_specifications)

    for result in maestro.query(Constructor("Evaluator"), 1000):
        print(deps_tree.print_tree(result))
        luigi.build([result], local_scheduler=True)


if __name__ == "__main__":
    main()
