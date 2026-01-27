from pathlib import Path

import luigi
from cosy import Constructor, Maestro, SpecificationBuilder
from luigi.tools import deps_tree


batching_required = False


########## loading ###############

class InstanceLoader(luigi.Task):

    def run(self):
        Path("InstanceLoader.txt").touch()

    def output(self):
        return luigi.LocalTarget("InstanceLoader.txt")


########## batching ##############

class Batching(luigi.Task):

    required_type = luigi.TaskParameter() # Constructor("InstanceLoader")

    def requires(self):
        return self.required_type

    def run(self):
        Path(f"Batching.txt").touch()

    def output(self):
        return luigi.LocalTarget(f"Batching.txt")


########## routing ###############

class Routing(luigi.Task):

    required_type = luigi.TaskParameter()  # Constructor("Batching") or Constructor("InstanceLoader")

    def requires(self):
        # If batching is not required, Routing requires InstanceLoader directly
        if not batching_required and isinstance(self.required_type, Constructor):
            return InstanceLoader()  # skip batching
        return self.required_type

    def run(self):
        Path("Routing.txt").touch()

    def output(self):
        return luigi.LocalTarget("Routing.txt")


########## evaluation ############

class Evaluator(luigi.Task):
    routing = luigi.TaskParameter() # Constructor("Routing")
    
    def requires(self):
        return self.routing

    def run(self):
        Path("Evaluator.txt").touch()

    def output(self):
        return luigi.LocalTarget("Evaluator.txt")


########## cosy wiring ###########

def main():
    
    batching_constructor = Constructor("Batching") if batching_required else Constructor("InstanceLoader")

    named_components_with_specifications = [
        (
            "InstanceLoader",
            lambda: InstanceLoader(),
            SpecificationBuilder()
            .suffix(Constructor("InstanceLoader"))
        ),
        (
            "Routing",
            lambda concrete: Routing(concrete),
            SpecificationBuilder()
            .argument("concrete", batching_constructor)
            .suffix(Constructor("Routing"))
        ),
        (
            "Evaluator",
            lambda concrete: Evaluator(concrete),
            SpecificationBuilder()
            .argument("concrete", Constructor("Routing"))
            .suffix(Constructor("Evaluator"))
        )
    ]

    if batching_required:
        named_components_with_specifications.append(
            (
                "Batching",
                lambda concrete: Batching(concrete),
                SpecificationBuilder()
                .argument("concrete", Constructor("InstanceLoader"))
                .suffix(Constructor("Batching"))
            )
        )

    maestro = Maestro(named_components_with_specifications)

    for result in maestro.query(Constructor("Evaluator"), 1000):
        print(deps_tree.print_tree(result))
        luigi.build([result], local_scheduler=True)


if __name__ == "__main__":
    main()
