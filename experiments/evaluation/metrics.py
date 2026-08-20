"""VBS, SBS, and regret calculations."""

from __future__ import annotations

import numpy as np
import pandas as pd

from data import GAP_TOL, Objective, REGRET_THRESHOLDS


# ===========================================================================
# Per-instance regret
# ===========================================================================

def add_instance_regrets(
    df: pd.DataFrame,
    objective: Objective,
    *,
    instance_col: str = "instance_name",
) -> pd.DataFrame:
    """Attach each instance's VBS value and every pipeline's regret to it.

    Regret is 0 for the VBS (and any pipeline tied with it within GAP_TOL).
    For minimized objectives: regret = (z - VBS) / VBS * 100.
    For maximized objectives: regret = (VBS - z) / VBS * 100 (relative)
    or VBS - z (percentage_points).
    """
    work = df.copy()

    work["vbs_value"] = work.groupby(instance_col)[objective.column].transform(
        "max" if objective.maximize else "min"
    )

    if objective.maximize:
        raw_regret = work["vbs_value"] - work[objective.column]
    else:
        raw_regret = work[objective.column] - work["vbs_value"]

    if objective.regret_mode == "relative":
        work["regret"] = raw_regret / work["vbs_value"] * 100.0
    elif objective.regret_mode == "percentage_points":
        work["regret"] = raw_regret
    else:
        raise ValueError(f"Unknown regret mode: {objective.regret_mode}")

    work.loc[work["regret"].abs() <= GAP_TOL, "regret"] = 0.0
    return work


def regret_matrix(
    work: pd.DataFrame,
    *,
    instance_col: str = "instance_name",
) -> pd.DataFrame:
    return work.pivot(index=instance_col, columns="strategy", values="regret")


# ===========================================================================
# VBS-set membership and fractional winner credits
# ===========================================================================

def add_vbs_membership(
    work: pd.DataFrame,
    *,
    instance_col: str = "instance_name",
) -> pd.DataFrame:
    out = work.copy()
    out["in_vbs"] = out["regret"] == 0.0
    out["vbs_size"] = out.groupby(instance_col)["in_vbs"].transform("sum")
    if (out["vbs_size"] == 0).any():
        raise ValueError("An instance has zero VBS members; check regret computation.")
    return out


def pipeline_winner_credits(
    work: pd.DataFrame,
    *,
    instance_col: str = "instance_name",
) -> pd.DataFrame:
    out = work.copy()
    out["winner_credit"] = np.where(
        out["in_vbs"], 1.0 / out["vbs_size"], 0.0
    )
    return out


def count_distinct_winners(
    work: pd.DataFrame,
    *,
    instance_col: str = "instance_name",
) -> int:
    return int(work.loc[work["in_vbs"], "strategy"].nunique())


# ===========================================================================
# SBS selection
# ===========================================================================

def empirical_quantile_higher(values: pd.Series, probability: float) -> float:
    if not 0.0 <= probability <= 1.0:
        raise ValueError("probability must lie in [0, 1].")
    return float(
        np.quantile(
            values.to_numpy(dtype=float),
            probability,
            method="higher",
        )
    )


def threshold_column(threshold: float) -> str:
    return f"Share > {threshold:g}%"


def selection_stats(
    matrix: pd.DataFrame,
    mean_runtime: pd.Series,
) -> dict:
    """Select the SBS (fastest among tied minimum mean regret) and summarize
    its instance-wise regret distribution."""
    mean_regret = matrix.mean(axis=0)
    best_mean = float(mean_regret.min())

    tied = mean_regret.index[
        np.isclose(mean_regret, best_mean, atol=GAP_TOL, rtol=0)
    ]
    sbs = mean_runtime.reindex(tied).sort_values(kind="mergesort").index[0]
    sbs_regret = matrix[sbs].astype(float)
    values = sbs_regret.to_numpy(dtype=float)

    stats = {
        "SBS": sbs,
        "Mean regret": float(sbs_regret.mean()),
        "p90 regret": empirical_quantile_higher(sbs_regret, 0.90),
        "Max regret": float(sbs_regret.max()),
        "# Inst.": int(matrix.shape[0]),
        "# Pipe.": int(matrix.shape[1]),
    }

    for threshold in REGRET_THRESHOLDS:
        stats[threshold_column(threshold)] = float(
            100.0 * np.mean(values > threshold + GAP_TOL)
        )

    return stats


def sbs_attainment_share(
    work: pd.DataFrame,
    sbs: str,
    *,
    instance_col: str = "instance_name",
) -> float:
    """Share of instances where the SBS belongs to the VBS set."""
    sbs_rows = work[work["strategy"] == sbs]
    n_instances = work[instance_col].nunique()
    n_attained = sbs_rows.loc[sbs_rows["regret"] == 0.0, instance_col].nunique()
    return 100.0 * n_attained / n_instances


def positive_regret_share(
    work: pd.DataFrame,
    sbs: str,
    *,
    instance_col: str = "instance_name",
) -> float:
    """Share of instances where the SBS has positive regret."""
    sbs_rows = work[work["strategy"] == sbs]
    n_instances = work[instance_col].nunique()
    n_positive = sbs_rows.loc[sbs_rows["regret"] > GAP_TOL, instance_col].nunique()
    return 100.0 * n_positive / n_instances


# ===========================================================================
# Reference-gap helpers
# ===========================================================================

def gap_min(df: pd.DataFrame, obj_col: str, ref_col: str = "reference_value") -> pd.Series:
    return ((df[obj_col] - df[ref_col]) / df[ref_col]) * 100


def reference_from_solvers(df: pd.DataFrame, cost_cols: list[str]) -> pd.DataFrame:
    """SPRP / SPRP-SS: best known value is min across exact solvers;
    optimality is certified by agreement."""
    present = [c for c in cost_cols if c in df.columns]
    if not present:
        raise KeyError(f"None of {cost_cols} found in results columns {list(df.columns)}")

    costs = df[present].astype(float)
    ref_value = costs.min(axis=1)
    agree = costs.round(6).nunique(axis=1) == 1

    df = df.copy()
    df["reference_value"] = ref_value
    df["reference_type"] = np.where(agree, "optimum", "reported feasible solution")
    return df


def reference_type_from_bounds(
    df: pd.DataFrame, time_col: str = "time [s]", time_limit: float = 3600.0
) -> pd.Series:
    hit_limit = df[time_col].astype(float) >= time_limit
    proven = (
        (df["UB"].round(6) == df["LB"].round(6))
        & (df["opt?"] == True)
        & (~hit_limit)
    )
    return np.where(proven, "optimum", "reported feasible solution")


def reference_type_from_lb_ub(df: pd.DataFrame) -> pd.Series:
    return np.where(
        df["UB"].round(6) == df["LB"].round(6),
        "optimum",
        "reported feasible solution",
    )
