from pathlib import Path

import luigi
from cosy import Constructor, Maestro, SpecificationBuilder, Var
from luigi.tools import deps_tree


########## start ###############

class Start(luigi.Task):
    
    def run(self):
        Path("Start.txt").touch()

    def output(self):
        return luigi.LocalTarget("Start.txt")

########## first ##############

class FirstA(luigi.Task): # -> First
    
    required_type = luigi.TaskParameter()
    
    def requires(self):
        return self.required_type
    
    def run(self):
        Path("FirstA.txt").touch()
    
    def output(self):
        return luigi.LocalTarget("FirstA.txt")

class FirstB(luigi.Task): # -> First
    
    required_type = luigi.TaskParameter()
    
    def requires(self):
        return self.required_type
    
    def run(self):
        Path("FirstB.txt").touch()

    def output(self):
        return luigi.LocalTarget("FirstB.txt")
    

########## second ###############
    
class SecondAlpha(luigi.Task): # -> Second
    required_type = luigi.TaskParameter()
    
    def requires(self):
        return self.required_type
    
    def run(self):
        Path("SecondAlpha.txt").touch()

    def output(self):
        return luigi.LocalTarget("SecondAlpha.txt")
    
class SecondBeta(luigi.Task): # -> Second
    required_type = luigi.TaskParameter()

    def requires(self):
        return self.required_type
    
    def run(self):
        Path("SecondBeta.txt").touch()

    def output(self):
        return luigi.LocalTarget("SecondBeta.txt")

class SecondAlphaA(luigi.Task): # -> SecondAlpha
    
    required_type = luigi.TaskParameter()
    
    def requires(self):
        return self.required_type
    
    def run(self):
        Path("SecondAlphaA.txt").touch()

    def output(self):
        return luigi.LocalTarget("SecondAlphaA.txt")
    
class SecondAlphaB(luigi.Task): # -> SecondAlpha
    
    required_type = luigi.TaskParameter()
    
    def requires(self):
        return self.required_type
    
    def run(self):
        Path("SecondAlphaB.txt").touch()

    def output(self):
        return luigi.LocalTarget("SecondAlphaB.txt")

class SecondBetaA(luigi.Task): # -> SecondBeta
    
    required_type = luigi.TaskParameter()
    
    def requires(self):
        return self.required_type
    
    def run(self):
        Path("SecondBetaA.txt").touch()

    def output(self):
        return luigi.LocalTarget("SecondBetaA.txt")

class TargetNode(luigi.Task): # -> Second
    required_type = luigi.TaskParameter()    
    
    def requires(self):
        return self.required_type
    
    def run(self):
        Path("TargetNode.txt").touch()

    def output(self):
        return luigi.LocalTarget("TargetNode.txt")    
    
########### end ###############

class End(luigi.Task):
    required_type = luigi.TaskParameter()    
    
    def requires(self):
        return self.required_type
    
    def run(self):
        Path("End.txt").touch()

    def output(self):
        return luigi.LocalTarget("End.txt")

    
def main():
    
    named_components_with_specifications = [

        #### start ####
        (
            "Start",
            lambda: Start(),
            SpecificationBuilder()
            .suffix(Constructor("Start")),
        ),
        
        #### first ####
        (
            "FirstA",
            lambda concrete: FirstA(concrete),
            SpecificationBuilder()
            .argument("concrete", Constructor("Start"))
            .suffix(Constructor("First"))
        ),
        (
            "FirstB",
            lambda concrete: FirstB(concrete),
            SpecificationBuilder()
            .argument("concrete", Constructor("Start"))
            .suffix(Constructor("First"))
        ),
        
        
        #### second ####
        
            ### alpha ###
        (
            "SecondAlphaA",
            lambda concrete: SecondAlphaA(concrete),
            SpecificationBuilder()
            .argument("concrete", Constructor("First"))
            .suffix(Constructor("SecondAlpha")),
        ),
        (
            "SecondAlphaB",
            lambda concrete: SecondAlphaB(concrete),
            SpecificationBuilder()
            .argument("concrete", Constructor("First"))
            .suffix(Constructor("SecondAlpha")),
        ),
            ### beta ###
            
        (
            "SecondBetaA",
            lambda concrete: SecondBetaA(concrete),
            SpecificationBuilder()
            .argument("concrete", Constructor("First"))
            .suffix(Constructor("SecondBeta")),
        ),
        
            ### target nodes ###
            
            # are used to make SecondAlpha and SecondBeta subclasses of Second, to pass to End as a single class
            # TODO possible to not add target node classes, but use logical OR?
        (
            "SecondAlphaTarget",
            lambda concrete: TargetNode(concrete),
            SpecificationBuilder()
            .argument("concrete", Constructor("SecondAlpha"))
            .suffix(Constructor("Second")),
        ),
        (
            "SecondBetaTarget",
            lambda concrete: TargetNode(concrete),
            SpecificationBuilder()
            .argument("concrete", Constructor("SecondBeta"))
            .suffix(Constructor("Second")),
        ),
        
        #### end ####
        (
            "End",
            lambda concrete: End(concrete),
            SpecificationBuilder()
            .argument("concrete", Constructor("Second"))
            .suffix(Constructor("End")),
        ),
    ]
    
    maestro = Maestro(named_components_with_specifications)
    
    for result in maestro.query(Constructor("End"), 1000):
        print(deps_tree.print_tree(result))
        luigi.build([result], local_scheduler=True)


if __name__ == "__main__":
    main()
