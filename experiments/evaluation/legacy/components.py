"""Helpers for grouping results by pipeline component."""

from __future__ import annotations

import numpy as np
import pandas as pd

from data import ALGO_COLS, BR_ALGOS, GAP_TOL, IAR_ALGOS
from portfolio import add_structure


# ===========================================================================
# Component classification
# ===========================================================================

# Columns used for each component restriction.
COMPONENT_COLS = {
    "IA": "item_assignment_algo",
    "Batching": "batching_algo",
    "Routing": "routing_algo",
    "IAR": "routing_algo",
    "BR": "routing_algo",
    "Scheduling": "scheduling_algo",
}

# Which components apply to each structure.
STRUCTURE_COMPONENTS = {
    "sequential": ["IA", "Batching", "Routing", "Scheduling"],
    "IAR": ["IAR", "Scheduling"],
    "BR": ["IA", "BR", "Scheduling"],
}


def sbs_components(work: pd.DataFrame, sbs: str) -> dict:
    """Extract the SBS's component values from its first row."""
    row = work[work["strategy"] == sbs].iloc[0]
    structure = row.get("structure", "sequential")
    comps: dict[str, str | float] = {}
    for comp_name in STRUCTURE_COMPONENTS.get(structure, []):
        col = COMPONENT_COLS[comp_name]
        val = row.get(col)
        comps[comp_name] = val
    comps["structure"] = structure
    return comps


# ===========================================================================
# Component-restricted residual regret
# ===========================================================================

def component_restricted_residual(
    work: pd.DataFrame,
    component_name: str,
    sbs_value: str | float,
    *,
    instance_col: str = "instance_name",
) -> float:
    """Best achievable mean regret when ``component_name`` is fixed to
    ``sbs_value``.

    Returns NaN if the component is absent (None) or no pipeline matches.
    """
    if pd.isna(sbs_value) or str(sbs_value).strip() == "":
        return float("nan")

    col = COMPONENT_COLS[component_name]
    subset = work[work[col] == sbs_value]
    if subset.empty:
        return float("nan")

    min_regret = subset.groupby(instance_col)["regret"].min()
    n_expected = work[instance_col].nunique()
    if min_regret.shape[0] != n_expected:
        raise ValueError(
            f"Component restriction on {component_name}={sbs_value}: "
            f"covers {min_regret.shape[0]} instances, expected {n_expected}."
        )
    return float(min_regret.mean())


def compute_component_restriction(
    work: pd.DataFrame,
    instance_set_display: str,
    *,
    instance_col: str = "instance_name",
) -> dict:
    """Compute full SBS regret and component-restricted residuals for one set.

    Handles sequential, IAR, and BR structures with structure-aware columns.
    """
    work = add_structure(work)
    matrix = work.pivot(index=instance_col, columns="strategy", values="regret")
    mean_runtime = work.groupby("strategy")["total_cpu_time"].mean()
    mean_regret = matrix.mean(axis=0)
    best_mean = float(mean_regret.min())
    tied = mean_regret.index[np.isclose(mean_regret, best_mean, atol=GAP_TOL, rtol=0)]
    sbs = mean_runtime.reindex(tied).sort_values(kind="mergesort").index[0]

    comps = sbs_components(work, sbs)
    structure = comps.pop("structure")

    full_mean = float(matrix[sbs].mean())
    n = int(matrix.shape[0])

    result: dict = {
        "Instance Set": instance_set_display,
        "SBS": sbs,
        "Structure": structure,
        "Full regret [%]": round(full_mean, 4),
        "# Inst.": n,
    }

    all_components = ["IA", "Batching", "Routing", "IAR", "BR", "Scheduling"]
    for comp_name in all_components:
        col_label = f"{comp_name} fixed [%]"
        if comp_name in STRUCTURE_COMPONENTS.get(structure, []):
            val = component_restricted_residual(work, comp_name, comps.get(comp_name))
            result[col_label] = round(val, 4) if not np.isnan(val) else None
        else:
            result[col_label] = None

    return result


# ===========================================================================
# Structure-level residual regret
# ===========================================================================

def structure_restricted_residual(
    work: pd.DataFrame,
    structure: str,
    *,
    instance_col: str = "instance_name",
) -> float:
    """Best achievable mean regret when the pipeline structure is fixed."""
    subset = work[work["structure"] == structure]
    if subset.empty:
        return float("nan")
    min_regret = subset.groupby(instance_col)["regret"].min()
    n_expected = work[instance_col].nunique()
    if min_regret.shape[0] != n_expected:
        raise ValueError(
            f"Structure restriction to {structure}: "
            f"covers {min_regret.shape[0]} instances, expected {n_expected}."
        )
    return float(min_regret.mean())


def exclude_structure_residual(
    work: pd.DataFrame,
    structure: str,
    *,
    instance_col: str = "instance_name",
) -> float:
    """Best achievable mean regret when one structure is excluded."""
    subset = work[work["structure"] != structure]
    if subset.empty:
        return float("nan")
    min_regret = subset.groupby(instance_col)["regret"].min()
    n_expected = work[instance_col].nunique()
    if min_regret.shape[0] != n_expected:
        return float("nan")
    return float(min_regret.mean())


# ===========================================================================
# Winner-credit aggregation to component level
# ===========================================================================

def component_winner_distribution(
    work: pd.DataFrame,
    instance_set_display: str,
    objective_label: str,
    *,
    instance_col: str = "instance_name",
) -> pd.DataFrame:
    """Aggregate fractional winner credits to the component level.

    Returns one row per (instance set, objective, component-type, component)
    with positive VBS participation. Integrated components (IAR, BR) appear
    under their own component type, not under the stages they replace.
    """
    work = add_structure(work)
    n_instances = work[instance_col].nunique()
    rows: list[dict] = []

    # Sequential components: IA, B, R, S.
    for comp_name, col in [
        ("IA", "item_assignment_algo"),
        ("Batching", "batching_algo"),
        ("Routing", "routing_algo"),
        ("Scheduling", "scheduling_algo"),
    ]:
        in_scope = work[work["structure"] == "sequential"]
        if comp_name == "Scheduling":
            in_scope = work[work["scheduling_algo"].notna() & (work["scheduling_algo"] != "")]
        if in_scope.empty:
            continue
        credits = (
            in_scope[in_scope["in_vbs"]]
            .groupby(col)["winner_credit"]
            .sum()
        )
        for component, credit in credits.items():
            if pd.isna(component) or str(component).strip() == "":
                continue
            rows.append({
                "Instance Set": instance_set_display,
                "Objective": objective_label,
                "Component Type": comp_name,
                "Component": str(component),
                "Winner share [%]": round(float(credit) / n_instances * 100, 2),
            })

    # Integrated IAR.
    iar_in_scope = work[(work["structure"] == "IAR") & work["in_vbs"]]
    if not iar_in_scope.empty:
        credits = iar_in_scope.groupby("routing_algo")["winner_credit"].sum()
        for component, credit in credits.items():
            rows.append({
                "Instance Set": instance_set_display,
                "Objective": objective_label,
                "Component Type": "IAR",
                "Component": str(component),
                "Winner share [%]": round(float(credit) / n_instances * 100, 2),
            })

    # Integrated BR.
    br_in_scope = work[(work["structure"] == "BR") & work["in_vbs"]]
    if not br_in_scope.empty:
        credits = br_in_scope.groupby("routing_algo")["winner_credit"].sum()
        for component, credit in credits.items():
            rows.append({
                "Instance Set": instance_set_display,
                "Objective": objective_label,
                "Component Type": "BR",
                "Component": str(component),
                "Winner share [%]": round(float(credit) / n_instances * 100, 2),
            })

    return pd.DataFrame(rows)


def structure_winner_distribution(
    work: pd.DataFrame,
    instance_set_display: str,
    objective_label: str,
    *,
    instance_col: str = "instance_name",
) -> pd.DataFrame:
    """Aggregate fractional winner credits to the structure level."""
    work = add_structure(work)
    n_instances = work[instance_col].nunique()
    credits = (
        work[work["in_vbs"]]
        .groupby("structure")["winner_credit"]
        .sum()
    )
    rows = []
    for structure, credit in credits.items():
        rows.append({
            "Instance Set": instance_set_display,
            "Objective": objective_label,
            "Structure": structure,
            "Winner share [%]": round(float(credit) / n_instances * 100, 2),
        })
    return pd.DataFrame(rows)
