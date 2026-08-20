# Benchmark instances

The full benchmark collections are not stored in Git. Obtain them from the
authors' pages, then place the prepared files under `data/instances/` using the
directory names below.

| Paper benchmark | Source | Prepared directory | Local preparation |
| --- | --- | --- | --- |
| SPRP | [Heßler--Irnich benchmark download](https://download.uni-mainz.de/fb03-logistikmanagement/benchmarks/instances_SPRP-SS_JOBPRP.zip) | `SPRP` | Extract the published SPRP files. |
| SPRP-SS | [SPRP-SS repository](https://github.com/katrinhessler/SPRP-SS) | `SPRP-SS` | Copy the published scattered-storage instances. |
| BahceciOencan | [Heßler--Irnich benchmark download](https://download.uni-mainz.de/fb03-logistikmanagement/benchmarks/instances_SPRP-SS_JOBPRP.zip) | `BahceciOencan` | Extract the published files. |
| HennWaescher | [Heßler--Irnich benchmark download](https://download.uni-mainz.de/fb03-logistikmanagement/benchmarks/instances_SPRP-SS_JOBPRP.zip) | `HennWaescherUniform`, `HennWaescherClassBased` | Run `scripts/prepare_henn_waescher.py`. |
| MuterOencan | [Heßler--Irnich benchmark download](https://download.uni-mainz.de/fb03-logistikmanagement/benchmarks/instances_SPRP-SS_JOBPRP.zip) | `MuterOencanWG` | Run `scripts/generate_muter_oencan_wg.py`. |
| Foodmart | [G-SCOP batching benchmark page](https://pagesperso.g-scop.grenoble-inp.fr/~cambazah/batching/) | `FoodmartData` | Extract the 144 published files. |
| Kris | [G-SCOP sequencing benchmark page](https://pagesperso.g-scop.grenoble-inp.fr/~cambazah/sequencing/) | `KrisSmallDataCorrected`, `KrisLargeData` | Extract the small and large collections. |

The [University of Mainz benchmark page](https://logistik.bwl.uni-mainz.de/research/#benchmarks)
provides the context for the Heßler--Irnich archive and links the SPRP-SS
results repository.

## HennWaescher preparation

Preserve the published HennWaescher directory tree below
`data/instances/HennWaescher`. The preparation script selects the Largest-Gap
and S-Shape subsets from both storage policies and flattens them into the two
directories expected by the runner:

```bash
uv run --frozen python scripts/prepare_henn_waescher.py
```

The script checks that each target contains 2,880 uniquely named files.

## MuterOencan preparation

Place the 270 original files in `data/instances/MuterOencan`, then run:

```bash
uv run --frozen python scripts/generate_muter_oencan_wg.py
```

The paper comparison uses the resulting `MuterOencanWG` files. The script
changes `DISTANCE_TOP_TO_CELL` and `DISTANCE_BOTTOM_TO_CELL` from 1 to 1.5 to
match the layout-distance parameters used by Wahlen and Gschwind; all other
instance data are copied unchanged.

## Kris directories

Place the 243 small instances in `KrisSmallDataCorrected` and the 237 large
instances in `KrisLargeData`. The small collection includes
`instances_220_1.txt`.

## Licensing

The SPRP-SS GitHub repository is distributed under the MIT License. The other
source pages do not state a separate redistribution license for their benchmark
downloads. They are therefore obtained directly from the cited sources and are
not republished as part of this software repository. Users are responsible for
following the terms of the original sources.
