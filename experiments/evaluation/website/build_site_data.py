from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd


# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

SCRIPT_DIR = Path(__file__).resolve().parent
EVALUATION_DIR = SCRIPT_DIR.parent

DF_RESULTS_PATH = EVALUATION_DIR / "df_results.pkl"
SITE_DIR = SCRIPT_DIR / "site"
SITE_DATA_DIR = SITE_DIR / "data"


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

STAGES = [
    "item_assignment",
    "batching",
    "routing",
    "scheduling",
]

METRIC_COLS = [
    "total_distance",
    "total_cpu_time",
    "makespan",
    "on_time_rate",
    "max_tardiness",
    "avg_tardiness",
    "avg_lateness",
    "max_lateness",
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def is_nonempty(value) -> bool:
    return pd.notna(value) and str(value) not in {"", "None", "nan"}


def short_fp(value, n: int = 8) -> str:
    if not is_nonempty(value):
        return ""
    return str(value)[:n]


def to_jsonable_records(df: pd.DataFrame) -> list[dict]:
    clean = df.replace({np.nan: None})
    return clean.to_dict(orient="records")


def write_json(path: Path, obj) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=2, default=str), encoding="utf-8")


# ---------------------------------------------------------------------------
# Data preparation
# ---------------------------------------------------------------------------

def load_df_results(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(
            f"Missing {path}. Run experiments/evaluation/build_results_cache.py first."
        )

    df = pd.read_pickle(path)
    if df.empty:
        raise RuntimeError(f"{path} exists but contains an empty dataframe.")

    return df


def add_missing_website_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    if "pipeline_chain_fingerprint" in df.columns and "pipeline_fp_short" not in df.columns:
        df["pipeline_fp_short"] = df["pipeline_chain_fingerprint"].map(short_fp)

    for stage in STAGES:
        fp_col = f"{stage}_algo_fingerprint"
        short_col = f"{stage}_algo_fp_short"

        if fp_col in df.columns and short_col not in df.columns:
            df[short_col] = df[fp_col].map(short_fp)

    if "strategy" not in df.columns:
        df["strategy"] = ""

    if "strategy_versioned" not in df.columns:
        df["strategy_versioned"] = df["strategy"]

    return df


def make_overview(df: pd.DataFrame) -> dict:
    overview = {
        "n_results": int(len(df)),
        "n_instance_sets": int(df["instance_set"].nunique()) if "instance_set" in df.columns else 0,
        "n_instances": int(df["instance_name"].nunique()) if "instance_name" in df.columns else 0,
        "n_strategies": int(df["strategy"].nunique()) if "strategy" in df.columns else 0,
        "n_strategy_versions": int(df["strategy_versioned"].nunique()) if "strategy_versioned" in df.columns else 0,
    }

    if "pipeline_chain_fingerprint" in df.columns:
        overview["n_pipeline_chains"] = int(df["pipeline_chain_fingerprint"].dropna().nunique())

    if "instance_set" in df.columns:
        overview["by_instance_set"] = (
            df.groupby("instance_set", dropna=False)
            .agg(
                n_results=("instance_name", "count"),
                n_instances=("instance_name", "nunique"),
                n_strategies=("strategy", "nunique"),
                n_strategy_versions=("strategy_versioned", "nunique"),
            )
            .reset_index()
            .to_dict(orient="records")
        )

    return overview


def make_version_overview(df: pd.DataFrame) -> list[dict]:
    rows = []

    for stage in STAGES:
        algo_col = f"{stage}_algo"
        fp_col = f"{stage}_algo_fingerprint"
        fp_short_col = f"{stage}_algo_fp_short"

        if algo_col not in df.columns or fp_col not in df.columns:
            continue

        tmp = df[[algo_col, fp_col]].copy()
        tmp = tmp[tmp[fp_col].map(is_nonempty)]

        if tmp.empty:
            continue

        grouped = (
            tmp.groupby([algo_col, fp_col], dropna=False)
            .size()
            .reset_index(name="n_results")
        )

        for _, row in grouped.iterrows():
            rows.append({
                "stage": stage,
                "algo": row[algo_col],
                "algo_fingerprint": row[fp_col],
                "algo_fp_short": short_fp(row[fp_col]),
                "n_results": int(row["n_results"]),
            })

    return rows


def make_performance_overview(df: pd.DataFrame) -> list[dict]:
    metric_cols = [c for c in METRIC_COLS if c in df.columns]
    if not metric_cols:
        return []

    group_cols = [
        c for c in [
            "instance_set",
            "strategy",
            "strategy_versioned",
            "pipeline_chain_fingerprint",
            "pipeline_fp_short",
        ]
        if c in df.columns
    ]

    work = df.copy()
    for col in metric_cols:
        work[col] = pd.to_numeric(work[col], errors="coerce")

    agg_spec = {
        metric: (metric, "mean")
        for metric in metric_cols
    }
    agg_spec["n_results"] = ("instance_name", "count")

    out = (
        work.groupby(group_cols, dropna=False)
        .agg(**agg_spec)
        .reset_index()
    )

    return to_jsonable_records(out)


def select_result_columns(df: pd.DataFrame) -> pd.DataFrame:
    cols = [
        "instance_set",
        "instance_name",
        "problem_type",
        "strategy",
        "strategy_versioned",
        "pipeline_chain_fingerprint",
        "pipeline_fp_short",
        "result_aggregation",
        "file_path",
    ]

    for stage in STAGES:
        cols.extend([
            f"{stage}_algo",
            f"{stage}_algo_raw",
            f"{stage}_algo_fingerprint",
            f"{stage}_algo_fp_short",
            f"{stage}_own_fingerprint",
            f"{stage}_chain_fingerprint",
            f"{stage}_time",
        ])

    cols.extend(METRIC_COLS)

    cols = [c for c in cols if c in df.columns]
    return df[cols].copy()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    df = load_df_results(DF_RESULTS_PATH)
    df = add_missing_website_columns(df)

    results = select_result_columns(df)

    write_json(SITE_DATA_DIR / "results.json", to_jsonable_records(results))
    write_json(SITE_DATA_DIR / "overview.json", make_overview(df))
    write_json(SITE_DATA_DIR / "version_overview.json", make_version_overview(df))
    write_json(SITE_DATA_DIR / "performance_overview.json", make_performance_overview(df))

    print(f"Read:  {DF_RESULTS_PATH}")
    print(f"Wrote: {SITE_DATA_DIR / 'results.json'}")
    print(f"Wrote: {SITE_DATA_DIR / 'overview.json'}")
    print(f"Wrote: {SITE_DATA_DIR / 'version_overview.json'}")
    print(f"Wrote: {SITE_DATA_DIR / 'performance_overview.json'}")


if __name__ == "__main__":
    main()