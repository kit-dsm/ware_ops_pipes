# Evaluation

Download `casop-results-v0.1.0.parquet` from release `v0.1.0`, rename it to
`df_results.parquet`, and place it in this directory. Its SHA-256 checksum is:

```
4E3DD80237636B5A29FF80B0409769768F7B0EABA3CEDFB45DF5D689DD8322AC
```

Run the paper evaluation from the repository root:

```bash
python experiments/evaluation/02_prepare_literature_references.py
python experiments/evaluation/03_compare_vbs_to_references.py
python experiments/evaluation/04_generate_paper_tables.py
python experiments/evaluation/05_generate_runtime_table.py
python experiments/evaluation/06_plot_foodmart_scaling.py
```

Tables are written to `tables/`; the Foodmart appendix figure is written to
`figures/`. Script `01_prepare_pipeline_results.py` rebuilds the Parquet summary
when the raw experiment outputs are available under `experiments/output/`.

Superseded evaluation code is under `legacy/`.
