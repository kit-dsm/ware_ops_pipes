"""Portfolio construction and pipeline counts."""

from __future__ import annotations

import pandas as pd

from data import (
    ALGO_COLS,
    BR_ALGOS,
    DISPLAY_INSTANCE_NAMES,
    DISTANCE,
    DISTANCE_SETS,
    INSTANCE_ORDER,
    IAR_ALGOS,
    KRIS_OBJECTIVES,
    Objective,
    is_missing_or_empty,
    to_numeric_nonempty,
)


# ===========================================================================
# Structure classification
# ===========================================================================

def pipeline_structure(row: pd.Series) -> str:
    """Classify a pipeline row as 'IAR', 'BR', or 'sequential'.

    IAR  integrated item-assignment-and-routing (routing_algo in IAR_ALGOS)
    BR   integrated batching-and-routing (routing_algo in BR_ALGOS)
    sequential  everything else (IA -> B -> R or any subset thereof)
    """
    routing = str(row.get("routing_algo", ""))
    if routing in IAR_ALGOS:
        return "IAR"
    if routing in BR_ALGOS:
        return "BR"
    return "sequential"


def add_structure(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["structure"] = out.apply(pipeline_structure, axis=1)
    return out


# ===========================================================================
# Complete-case portfolio
# ===========================================================================

def prepare_complete_portfolio(
    df: pd.DataFrame,
    objective: Objective,
    *,
    instance_col: str = "instance_name",
    context: str = "",
) -> pd.DataFrame:
    """Keep pipelines with a valid objective value on every retained instance.

    The VBS and SBS are computed over the same complete portfolio. Pipelines
    that did not run on every instance are excluded so that mean regret is
    comparable across pipelines.
    """
    work = df.copy()
    work[objective.column] = to_numeric_nonempty(work[objective.column])
    work = work.dropna(subset=[objective.column, "strategy", instance_col])

    if work.empty:
        raise ValueError(f"No usable rows for {context}/{objective.label}.")

    if objective.regret_mode == "relative":
        invalid = work[objective.column] <= 0
        if invalid.any():
            invalid_instances = work.loc[invalid, instance_col].drop_duplicates()
            print(
                f"[portfolio] {context}/{objective.label}: excluding "
                f"{len(invalid_instances)} invalid instance(s) with non-positive "
                f"objective values."
            )
            work = work[~work[instance_col].isin(invalid_instances)].copy()

    if work.empty:
        raise ValueError(f"No valid instances for {context}/{objective.label}.")

    n_instances = work[instance_col].nunique()
    coverage = work.groupby("strategy")[instance_col].nunique()
    complete = coverage[coverage == n_instances].index

    n_excluded = int((coverage < n_instances).sum())
    if n_excluded:
        best_incomplete = int(coverage[coverage < n_instances].max())
        print(
            f"[portfolio] {context}/{objective.label}: excluding {n_excluded} "
            f"incomplete pipeline(s); best coverage "
            f"{best_incomplete}/{n_instances}."
        )

    if complete.empty:
        raise ValueError(
            f"{context}/{objective.label}: no pipeline has a result on all "
            f"{n_instances} retained instances."
        )

    return work[work["strategy"].isin(complete)].copy()


# ===========================================================================
# Pipeline-count overview
# ===========================================================================

def nonempty_nunique(series: pd.Series) -> int:
    return int(series.replace("", pd.NA).dropna().nunique())


def compute_pipeline_results_overview(df: pd.DataFrame) -> pd.DataFrame:
    """Count configured algorithms and evaluated pipelines per instance set.

    Columns: IA, R, B, IAR, BR, S, n_instances, n_pipelines.
    """
    df = add_structure(df)
    grouped = df.groupby("instance_set", dropna=False)

    rows: list[dict] = []
    for name, sub in grouped:
        n_ia = nonempty_nunique(sub.loc[sub["structure"] == "sequential", "item_assignment_algo"])
        n_r = nonempty_nunique(sub.loc[sub["structure"] == "sequential", "routing_algo"])
        n_b = nonempty_nunique(sub.loc[sub["structure"] != "BR", "batching_algo"])
        n_iar = sub.loc[sub["structure"] == "IAR", "routing_algo"].nunique()
        n_br = sub.loc[sub["structure"] == "BR", "routing_algo"].nunique()
        n_s = nonempty_nunique(sub["scheduling_algo"])
        rows.append({
            "instance_set": name,
            "IA": n_ia,
            "R": n_r,
            "B": n_b,
            "IAR": n_iar,
            "BR": n_br,
            "S": n_s,
            "n_instances": sub["instance_name"].nunique(),
            "n_pipelines": len(sub),
        })

    overview = pd.DataFrame(rows).set_index("instance_set")
    return overview[["IA", "R", "B", "IAR", "BR", "S", "n_instances", "n_pipelines"]]
