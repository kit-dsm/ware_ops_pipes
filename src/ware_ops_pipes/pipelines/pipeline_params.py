from os import getcwd
from os.path import join as pjoin
import luigi


class PipelineParams(luigi.Config):

    output_folder = luigi.Parameter(default=pjoin(getcwd(), "outputs"))
    seed = luigi.IntParameter(default=42)

    time_limit_sec = luigi.OptionalIntParameter(default=None)
    gen_tour = luigi.BoolParameter(default=False)

    instance_set_name = luigi.Parameter(default=None)
    instance_name = luigi.Parameter(default=None)
    instance_path = luigi.Parameter(default=None)
    domain_path = luigi.Parameter(default=None)


