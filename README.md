# Context-Aware Synthesis of Optimization Pipelines

`ware_ops_pipes` contains the pipeline synthesis and evaluation code for
CASOP, the framework presented in *Context-Aware Synthesis of Optimization
Pipelines for Warehouse Optimization*. CASOP identifies applicable algorithm
configurations and synthesizes executable optimization pipelines for item
assignment, batching, routing, and scheduling.

The repository works with [`ware_ops_algos`](https://github.com/kit-dsm/ware_ops_algos),
which provides the common domain model, data loaders, algorithm implementations,
algorithm cards, and problem taxonomy. `ware_ops_pipes` uses the algorithm cards
and data cards to identify applicable algorithms, composes valid pipelines with
[`CLS-Luigi`](https://github.com/cls-python/cls-luigi), executes them, and ranks
their results by the objective declared in the data card.

## Repository structure

- `src/ware_ops_pipes/`: pipeline components, synthesis, execution, and ranking
- `data/data_cards/`: data cards for the benchmark sets
- `data/instances/`: local benchmark directory; full benchmark collections are
  not stored in Git
- `experiments/`: runners and the paper evaluation workflow
- `scripts/`: deterministic benchmark preparation scripts
- `examples/`: small entry points for inspecting and running CASOP

The five building blocks described in the paper are Data Cards and Domain
Objects, Algorithm Cards and Algorithm Repository, Problem Taxonomy, Pipeline
Synthesizer, and Pipeline Evaluator. The first three are implemented primarily
in `ware_ops_algos`; the last two are implemented here.

The maintained [documentation](https://kit-dsm.github.io/ware_ops_pipes/)
introduces pipeline synthesis and shows how to add a pipeline component.

## Installation

Python 3.11 or newer and [uv](https://docs.astral.sh/uv/) are required for the
reproduction commands below. The exact `ware_ops_algos` revision is pinned in
both `pyproject.toml` and `uv.lock`. `uv` is the canonical tool for installing
the locked environment and running the examples, experiments, and evaluation.

```bash
git clone https://github.com/kit-dsm/ware_ops_pipes.git
cd ware_ops_pipes
uv sync --frozen --extra eval
```

Run the self-contained applicability example to check the installation:

```bash
uv run --frozen python examples/list_applicable_algorithms.py
```

### Gurobi

`gurobipy` is installed through `ware_ops_algos`. A Gurobi license is required
for the exact routing and integrated batching-routing configurations. The
heuristic applicability example above does not solve a Gurobi model. The full
BahceciOencan experiment includes the integrated exact configuration and
therefore requires a license that supports the instance sizes. Gurobi provides
[Python installation instructions](https://support.gurobi.com/hc/en-us/articles/360044290292-How-do-I-install-Gurobi-for-Python)
and [academic licensing information](https://support.gurobi.com/hc/en-us/articles/12684663118993-How-do-I-obtain-a-Gurobi-license).

## Examples

`examples/list_applicable_algorithms.py` loads the Foodmart data card and lists
the compatible executable algorithm configurations. To synthesize and execute
pipelines for one Foodmart instance after obtaining the benchmark data, run:

```bash
uv run --frozen python examples/run_one_foodmart_instance.py data/instances/FoodmartData/instances_d5_ord5_MAL.txt --max-pipelines 3
```

The second example connects the `ware_ops_algos` loader and algorithm cards to
the CASOP pipeline synthesizer. See the
[`ware_ops_algos` examples](https://github.com/kit-dsm/ware_ops_algos/tree/main/examples)
for direct use of the domain model and algorithms, including a small custom
batching implementation.

## Benchmark data

The experiments use seven established instance sets covering the single-picker
routing problem (SPRP and SPRP-SS), the order batching and routing problem
(BahceciOencan, HennWaescher, MuterOencan, and Foodmart), and the order batching,
scheduling, and routing problem (Kris).

| Instance set | Citation | Prepared directory |
| --- | --- | --- |
| SPRP | Heßler and Irnich (2024), [doi:10.1287/ijoc.2023.0075](https://doi.org/10.1287/ijoc.2023.0075) | `data/instances/SPRP` |
| SPRP-SS | Heßler and Irnich (2024), [doi:10.1287/ijoc.2023.0075](https://doi.org/10.1287/ijoc.2023.0075) | `data/instances/SPRP-SS` |
| BahceciOencan | Bahçeci and Öncan (2022), [doi:10.1080/00207543.2021.1973684](https://doi.org/10.1080/00207543.2021.1973684) | `data/instances/BahceciOencan` |
| HennWaescher | Henn et al. (2010), [doi:10.1007/BF03342717](https://doi.org/10.1007/BF03342717) | `data/instances/HennWaescherUniform` and `data/instances/HennWaescherClassBased` |
| MuterOencan | Muter and Öncan (2015), [doi:10.1080/0740817X.2014.991478](https://doi.org/10.1080/0740817X.2014.991478) | `data/instances/MuterOencanWG` |
| Foodmart | Valle et al. (2017), [doi:10.1016/j.ejor.2017.03.069](https://doi.org/10.1016/j.ejor.2017.03.069) | `data/instances/FoodmartData` |
| Kris | Briant et al. (2023), [arXiv:2303.17834](https://arxiv.org/abs/2303.17834) | `data/instances/KrisSmallDataCorrected` and `data/instances/KrisLargeData` |

Download locations, expected directory structures, preparation steps, and
third-party licensing information are documented in
[`data/instances/README.md`](data/instances/README.md). In particular, the
HennWaescher files are flattened into the two directories used by the runner,
and the original MuterOencan instances are converted to the aisle-entry
distances used by Wahlen and Gschwind. Both transformations are automated.

## Reproducing the experiments

After preparing the benchmark directories, run the following commands from the
repository root. Each runner processes every `.txt` file in the selected
directory and writes instance-level outputs below `experiments/output/`.

```bash
uv run --frozen python experiments/run_hessler_irnich.py SPRP --workers 1
uv run --frozen python experiments/run_hessler_irnich.py SPRP-SS --workers 1
uv run --frozen python experiments/run_hessler_irnich.py BahceciOencan --workers 1
uv run --frozen python experiments/run_hessler_irnich.py HennWaescherUniform --workers 1
uv run --frozen python experiments/run_hessler_irnich.py HennWaescherClassBased --workers 1
uv run --frozen python experiments/run_hessler_irnich.py MuterOencanWG --workers 1
uv run --frozen python experiments/run_foodmart.py --workers 1
uv run --frozen python experiments/run_ibrsp.py KrisSmallDataCorrected --workers 1
uv run --frozen python experiments/run_ibrsp.py KrisLargeData --workers 1
```

On Linux, all runners can also be started sequentially with:

```bash
bash experiments/all_experiments.sh --workers 1
```

These are large computational experiments. The paper results were produced on
the remote system described in the manuscript. Raw per-pipeline outputs are not
distributed because they occupy hundreds of gigabytes. The release Parquet file
is the instance-level summary consumed by the evaluation scripts.

## Reproducing the paper tables and figure

Download the canonical summary directly into the evaluation directory:

```bash
curl -L https://github.com/kit-dsm/ware_ops_pipes/releases/download/v0.1.0/casop-results-v0.1.0.parquet -o experiments/evaluation/df_results.parquet
```

The expected SHA-256 checksum is
`4E3DD80237636B5A29FF80B0409769768F7B0EABA3CEDFB45DF5D689DD8322AC`.
Then run:

```bash
uv run --frozen python experiments/evaluation/02_prepare_literature_references.py
uv run --frozen python experiments/evaluation/03_compare_vbs_to_references.py
uv run --frozen python experiments/evaluation/04_generate_paper_tables.py
uv run --frozen python experiments/evaluation/05_generate_runtime_table.py
uv run --frozen python experiments/evaluation/06_plot_foodmart_scaling.py
```

Generated tables are written to `experiments/evaluation/tables/`; the appendix
figure is written to `experiments/evaluation/figures/`. If the raw experiment
outputs are available under `experiments/output/`, rebuild the summary first:

```bash
uv run --frozen python experiments/evaluation/01_prepare_pipeline_results.py
```

The literature reference inputs and their provenance are described in
[`data/reference/README.md`](data/reference/README.md). The numbered workflow is
summarized in [`experiments/evaluation/README.md`](experiments/evaluation/README.md).

## Extending CASOP

An additional algorithm is implemented in `ware_ops_algos` together with its
algorithm card. A corresponding CLS-Luigi component in `ware_ops_pipes` exposes
the implementation to pipeline synthesis. The existing FIFO batching files are
compact reference points:

- [algorithm and interface](https://github.com/kit-dsm/ware_ops_algos/blob/main/src/ware_ops_algos/algorithms/batching/batching.py)
- [algorithm card](https://github.com/kit-dsm/ware_ops_algos/blob/main/src/ware_ops_algos/algorithms/algorithm_cards/fifo_batching.yaml)
- [pipeline component](src/ware_ops_pipes/pipelines/subproblems/batching/fifo.py)

The custom batching example in `ware_ops_algos/examples/custom_batching.py`
shows the algorithm-interface part without changing either repository.

[`examples/extend_with_custom_batching.py`](examples/extend_with_custom_batching.py)
shows the corresponding pipeline-component wrapper and verifies that CLS-Luigi
registers it for synthesis.

## License and citation

CASOP source code in this repository is licensed under the BSD 3-Clause
License. The benchmark files and literature result tables remain subject to
their source terms; see the data READMEs before redistributing them.

Citation metadata and the complete author list are provided in
[`CITATION.cff`](CITATION.cff) and [`AUTHORS`](AUTHORS). If you use CASOP,
please cite the accompanying paper and the archived software release.
