from typing import Union, List

from luigi.task import flatten

from cls.fcl import FiniteCombinatoryLogic
from cls.subtypes import Subtypes
from cls_luigi.inhabitation_task import RepoMeta

from ware_ops_pipes.pipelines.pipeline_params import PipelineParams


def inhabit(start: Union[RepoMeta, List[RepoMeta]]):
    target = [start] if isinstance(start, RepoMeta) else start
    target = [c.return_type() for c in target]
    print("Collecting repository...")
    repository = RepoMeta.repository
    print("Build repository...")
    fcl = FiniteCombinatoryLogic(repository, Subtypes(RepoMeta.subtypes), processes=1)
    print("Building tree grammar and inhabiting pipelines...")
    inhabitation_result = fcl.inhabit(*target)
    inhabitation_size = inhabitation_result.size()
    return inhabitation_result, inhabitation_size


def print_tree(task, indent='', last=True):
    name = task.__class__.__name__
    result = '\n' + indent
    if (last):
        result += '└─--'
        indent += '    '
    else:
        result += '|---'
        indent += '|   '
    result += '[{0}]'.format(name)
    print("Task", task)
    print("Requires", task.requires())
    children = flatten(task.requires())
    for index, child in enumerate(children):
        result += print_tree(child, indent, (index + 1) == len(children))
    return result


def set_pipeline_params(
    instance_set_name,
    instance_name,
    instance_path,
    output_folder,
    domain_path

) -> None:


    global_parameters = PipelineParams()
    global_parameters.output_folder = output_folder
    global_parameters.instance_set_name = instance_set_name
    global_parameters.instance_name = instance_name
    global_parameters.instance_path = instance_path
    global_parameters.domain_path = domain_path