# Evaluation

Download `casop-results-v0.1.0.parquet` from release `v0.1.0` as
`df_results.parquet` in this directory:

```bash
curl -L https://github.com/kit-dsm/ware_ops_pipes/releases/download/v0.1.0/casop-results-v0.1.0.parquet -o experiments/evaluation/df_results.parquet
```

Its SHA-256 checksum is:

```
4E3DD80237636B5A29FF80B0409769768F7B0EABA3CEDFB45DF5D689DD8322AC
```

Run the paper evaluation from the repository root:

```bash
uv run --frozen python experiments/evaluation/02_prepare_literature_references.py
uv run --frozen python experiments/evaluation/03_compare_vbs_to_references.py
uv run --frozen python experiments/evaluation/04_generate_paper_tables.py
uv run --frozen python experiments/evaluation/05_generate_runtime_table.py
uv run --frozen python experiments/evaluation/06_plot_foodmart_scaling.py
```

Tables are written to `experiments/evaluation/tables/`; the Foodmart appendix
figure is written to `experiments/evaluation/figures/`. Script
`01_prepare_pipeline_results.py` rebuilds the Parquet summary when the raw
experiment outputs are available under `experiments/output/`.

Superseded evaluation code is under `legacy/`.
