# Benchmark instances

The full benchmark collections are kept outside Git. Place prepared instances
under `data/instances/` using these directory names:

| Paper benchmark | Directory | Preparation |
| --- | --- | --- |
| SPRP | `SPRP` | Extract the published files directly. |
| SPRP-SS | `SPRP-SS` | Extract the published scattered-storage files directly. |
| Bahceci--Öncan | `BahceciOencan` | Extract the published files directly. |
| Henn--Wäscher | `HennWaescherUniform`, `HennWaescherClassBased` | Run `scripts/prepare_henn_waescher.py`. |
| Muter--Öncan | `MuterOencanWG` | Run `scripts/generate_muter_oencan_wg.py`. |
| Foodmart | `FoodmartData` | Extract the 144 published files directly. |
| Kris | `KrisSmallDataCorrected`, `KrisLargeData` | Extract the small and large published files into the corresponding directories. |

SPRP, SPRP-SS, Bahceci--Öncan, Henn--Wäscher, and the original Muter--Öncan
instances are available from the [University of Mainz benchmark
page](https://logistik.bwl.uni-mainz.de/research/#benchmarks). Foodmart and Kris
are available from the [G-SCOP batching](https://pagesperso.g-scop.grenoble-inp.fr/~cambazah/batching/)
and [sequencing](https://pagesperso.g-scop.grenoble-inp.fr/~cambazah/sequencing/)
pages. Preserve the directory structure from the Henn--Wäscher download under
`data/instances/HennWaescher`, then flatten its Largest-Gap and S-Shape subsets
for the loader with:

```bash
python scripts/prepare_henn_waescher.py
```

This creates 2,880 uniform and 2,880 class-based files. The Kris directories
must contain 243 small and 237 large instances; the current small collection
includes `instances_220_1.txt`.

The Muter--Öncan instances used in the paper have the Wahlen--Gschwind
aisle-entry distances. After placing the original instances in
`data/instances/MuterOencan`, generate this variant by running:

```bash
python scripts/generate_muter_oencan_wg.py
```

The script writes the converted files to `data/instances/MuterOencanWG`.
