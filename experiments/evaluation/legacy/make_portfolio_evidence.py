"""
Portfolio-diversity diagnostics for the CASOP experiments.

Generates three reproducible appendix tables from the existing canonical
result dataframe (``df_results.pkl``), using the same complete-portfolio
filtering as ``make_paper_results_risk.py`` so that all numbers are
consistent with the SBS-regret table in the main text.

Tables
------
Table A  VBS winner distributions (component level, with fractional tie
         handling).  Full pipeline-level detail is written to CSV.
Table B  SBS complementarity and regret exceedance.
Table C  Stage-restricted residual regret (OBRP instance sets).

Tie handling
-------------
VBS set membership, SBS attainment, and positive regret all use the same
documented tolerance as ``make_paper_results_risk.py``:

    GAP_TOL = 1e-9   (on the regret percentage)

After ``add_instance_regrets`` zeroes numerical noise, a pipeline belongs to
the VBS set on instance *i* iff its regret is exactly 0.  For the evaluated
integer-valued objectives (distances) this is equivalent to exact equality;
for floating-point objectives (times) the same tolerance is retained for
consistency with the published regret statistics.

Fractional winner credits
    w_{lambda,i} = 1 / |W_i|   if lambda in W_i
    w_{lambda,i} = 0            otherwise

where W_i is the VBS set on instance i.  Pipeline- and component-level winner
shares aggregate these fractional credits and sum to 100 % within each
instance set, objective, and stage.

Stage-restricted residual regret
    Fix one stage to the SBS's component, allow all other stages to vary
    freely, and take the best achievable (minimum) per-instance regret.
    The mean of these per-instance minima is the residual mean regret.
    These residuals are NOT additive contributions; pipeline stages interact.

Outputs
-------
    tables/appendix_winner_distribution.tex   Table A (component level)
    tables/appendix_winner_distribution.csv   Table A (component level)
    tables/appendix_winner_pipelines.csv      Full pipeline-level detail
    tables/appendix_regret_exceedance.tex     Table B
    tables/appendix_regret_exceedance.csv     Table B
    tables/appendix_stage_restriction.tex     Table C
    tables/appendix_stage_restriction.csv     Table C

No experiments are run; raw result files are not modified.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd

# Reuse the authoritative SBS-regret analysis functions.
sys.path.insert(0, str(Path(__file__).resolve().parent))
from make_paper_results_risk import (  # noqa: E402
    ALGO_COLS,
    DISPLAY_INSTANCE_NAMES,
    DISTANCE,
    DISTANCE_SETS,
    GAP_TOL,
    INSTANCE_ORDER,
    KRIS_OBJECTIVES,
    REGRET_THRESHOLDS,
    Objective,
    add_instance_regrets,
    build_strategy,
    empirical_quantile_higher,
    is_missing_or_empty,
    kris_frame,
    latex_escape,
    prepare_complete_portfolio,
    prepare_df_results,
    to_numeric_nonempty,
)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

# Stage columns and display labels, in pipeline order.
STAGE_COLS = [
    ("item_assignment_algo", "Item assignment"),
    ("batching_algo", "Batching"),
    ("routing_algo", "Routing"),
    ("scheduling_algo", "Scheduling"),
]

# OBRP instance sets (for the stage-restriction table).
OBRP_SETS = [
    "BahceciOencan",
    "HennWaescher",
    "MuterOencan",
    "FoodmartData",
]

# Kris objective display labels.
KRIS_OBJECTIVE_LABELS = {
    "total picking time": "total picking time",
    "makespan": "makespan",
    "on-time rate (pp)": "on-time rate (pp)",
}

# Objective units for table headers.
OBJECTIVE_UNITS = {
    "distance": r"\%",
    "total picking time": r"\%",
    "makespan": r"\%",
    "on-time rate (pp)": "pp",
}

# NaN display string for LaTeX tables.
NA_STR = "---"

# Assertion tolerance for winner-share sums.  Fractional credits 1/|W_i|
# accumulate floating-point rounding errors, so a loose tolerance is needed.
SUM_TOL = 1e-3


# ---------------------------------------------------------------------------
# VBS set, winner credits, and # Winners
# ---------------------------------------------------------------------------

def add_vbs_membership(
    work: pd.DataFrame,
    objective: Objective,
    *,
    instance_col: str = "instance_name",
) -> pd.DataFrame:
    """
    Tag each row with ``in_vbs`` (True iff the pipeline belongs to the VBS
    set on that instance) and ``vbs_size`` (|W_i|, the number of tied
    pipelines).  Uses the regret column produced by ``add_instance_regrets``.
    """
    out = work.copy()
    out["in_vbs"] = out["regret"] == 0.0
    out["vbs_size"] = out.groupby(instance_col)["in_vbs"].transform("sum")
    # Guard: every instance must have at least one VBS member.
    if (out["vbs_size"] == 0).any():
        raise ValueError("An instance has zero VBS members; check regret computation.")
    return out


def pipeline_winner_credits(
    work: pd.DataFrame,
    *,
    instance_col: str = "instance_name",
) -> pd.DataFrame:
    """
    Fractional winner credit per (instance, pipeline):
        1 / |W_i|  if the pipeline is in W_i, else 0.
    """
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
    """Number of distinct pipelines in the VBS set on at least one instance."""
    return int(work.loc[work["in_vbs"], "strategy"].nunique())


# ---------------------------------------------------------------------------
# Table A: component-level VBS winner distribution
# ---------------------------------------------------------------------------

def component_winner_distribution(
    work: pd.DataFrame,
    instance_set_display: str,
    objective_label: str,
    *,
    instance_col: str = "instance_name",
) -> pd.DataFrame:
    """
    Aggregate fractional winner credits to the component (stage) level.

    Returns one row per (instance set, objective, stage, component) with
    positive VBS participation.
    """
    n_instances = work[instance_col].nunique()
    rows: list[dict] = []

    for stage_col, stage_label in STAGE_COLS:
        if stage_col not in work.columns:
            continue
        stage_data = work[
            ["strategy", stage_col, "in_vbs", "winner_credit", instance_col]
        ].copy()

        # Skip stages where every pipeline has a missing value (e.g.,
        # scheduling for distance sets, item assignment for integrated IAR).
        if is_missing_or_empty(stage_data[stage_col]).all():
            continue

        # Label missing/empty components as "(integrated)" so that all VBS
        # pipelines are accounted for and shares sum to 100%.  This happens
        # for CBR pipelines, where batching is subsumed by the integrated
        # routing component.
        missing_mask = is_missing_or_empty(stage_data[stage_col])
        stage_data.loc[missing_mask, stage_col] = "(integrated)"

        # # VBS instances: instances where this component appears in >= 1 VBS pipeline.
        vbs_instances = (
            stage_data[stage_data["in_vbs"]]
            .groupby(stage_col)[instance_col]
            .nunique()
        )

        # Winner share: sum of fractional credits / n_instances * 100.
        credit_sum = stage_data.groupby(stage_col)["winner_credit"].sum()

        for component, n_vbs_inst in vbs_instances.items():
            share = 100.0 * credit_sum.get(component, 0.0) / n_instances
            rows.append(
                {
                    "Instance Set": instance_set_display,
                    "Objective": objective_label,
                    "Stage": stage_label,
                    "Component": str(component),
                    "# VBS Inst.": int(n_vbs_inst),
                    "Winner Share [%]": round(share, 4),
                }
            )

    table = pd.DataFrame(rows)
    if table.empty:
        return table

    # Sort by instance set, stage order, then winner share descending.
    stage_order = {label: i for i, (_, label) in enumerate(STAGE_COLS)}
    table["_stage_order"] = table["Stage"].map(stage_order)
    table = (
        table.sort_values(
            ["_stage_order", "Winner Share [%]"], ascending=[True, False]
        )
        .drop(columns="_stage_order")
        .reset_index(drop=True)
    )
    return table


def pipeline_winner_distribution(
    work: pd.DataFrame,
    instance_set_display: str,
    objective_label: str,
    *,
    instance_col: str = "instance_name",
) -> pd.DataFrame:
    """Full pipeline-level winner distribution for CSV output."""
    n_instances = work[instance_col].nunique()
    rows: list[dict] = []

    for strategy, grp in work.groupby("strategy"):
        in_vbs = grp["in_vbs"]
        n_vbs = int(in_vbs.sum())
        credit = float(grp["winner_credit"].sum())
        share = 100.0 * credit / n_instances
        rows.append(
            {
                "Instance Set": instance_set_display,
                "Objective": objective_label,
                "Strategy": strategy,
                "# VBS Inst.": n_vbs,
                "Winner Share [%]": round(share, 4),
                "Winner Credit": round(credit, 6),
            }
        )

    table = pd.DataFrame(rows)
    if table.empty:
        return table
    return table.sort_values("Winner Share [%]", ascending=False).reset_index(drop=True)


def assert_winner_shares_sum(
    comp_table: pd.DataFrame, instance_set_display: str, objective_label: str
) -> None:
    """Assert that component-level winner shares sum to 100% per stage."""
    if comp_table.empty:
        return
    subset = comp_table[
        (comp_table["Instance Set"] == instance_set_display)
        & (comp_table["Objective"] == objective_label)
    ]
    for stage_label, stage_grp in subset.groupby("Stage"):
        total = stage_grp["Winner Share [%]"].sum()
        if abs(total - 100.0) > SUM_TOL:
            raise AssertionError(
                f"{instance_set_display}/{objective_label}/{stage_label}: "
                f"winner shares sum to {total:.4f}, expected 100.0"
            )


def make_winner_distribution_latex(comp_table: pd.DataFrame) -> str:
    """Render the component-level winner distribution as a longtable."""
    if comp_table.empty:
        return "% Winner distribution table is empty.\n"

    lines = [
        r"{\footnotesize",
        r"\begin{longtable}{@{}llllrr@{}}",
        r"\caption{Component-level VBS winner distribution. "
        r"``\# VBS Inst.'' is the number of instances on which the component "
        r"appears in at least one VBS-attaining pipeline. ``Winner Share'' "
        r"uses fractional credits $1/|W_i|$ per tied VBS pipeline and sums "
        r"to 100\% within each instance set and stage.}"
        + "\n"
        + r"\label{tab:appendix_winner_distribution}",
        r"\\",
        r"\toprule",
        r"Instance Set & Objective & Stage & Component & \# VBS Inst. & Share [\%] \\",
        r"\midrule",
        r"\endfirsthead",
        r"\multicolumn{6}{c}{{\tablename\ \thetable{} -- continued}} \\",
        r"\toprule",
        r"Instance Set & Objective & Stage & Component & \# VBS Inst. & Share [\%] \\",
        r"\midrule",
        r"\endhead",
        r"\midrule",
        r"\multicolumn{6}{r}{Continued on next page} \\",
        r"\midrule",
        r"\endfoot",
        r"\bottomrule",
        r"\endlastfoot",
    ]

    prev_set: str | None = None
    prev_obj: str | None = None
    prev_stage: str | None = None

    for _, row in comp_table.iterrows():
        iset = row["Instance Set"]
        obj = row["Objective"]
        stage = row["Stage"]

        set_cell = rf"\textit{{{latex_escape(iset)}}}" if iset != prev_set else ""
        obj_cell = latex_escape(obj) if (obj != prev_obj or iset != prev_set) else ""
        stage_cell = latex_escape(stage) if stage != prev_stage else ""

        lines.append(
            " & ".join(
                [
                    set_cell,
                    obj_cell,
                    stage_cell,
                    latex_escape(row["Component"]).replace("+", " + "),
                    f"{int(row['# VBS Inst.']):,}",
                    f"{row['Winner Share [%]']:.1f}",
                ]
            )
            + r" \\"
        )
        prev_set, prev_obj, prev_stage = iset, obj, stage

    lines.append(r"\end{longtable}")
    lines.append(r"}")
    return "\n".join(lines) + "\n"


# ---------------------------------------------------------------------------
# Table B: SBS complementarity and regret exceedance
# ---------------------------------------------------------------------------

def sbs_complementarity_stats(
    work: pd.DataFrame,
    objective: Objective,
    *,
    instance_col: str = "instance_name",
) -> dict:
    """
    Compute SBS attainment, positive-regret rate, threshold-exceedance rates,
    and regret distribution statistics.

    Reuses the SBS selection logic from ``make_paper_results_risk.py``:
    smallest mean regret, ties broken by lower mean runtime.
    """
    matrix = work.pivot(index=instance_col, columns="strategy", values="regret")
    if matrix.isna().any().any():
        raise ValueError("Incomplete regret matrix after complete-portfolio filtering.")

    mean_runtime = work.groupby("strategy")["total_cpu_time"].mean()

    mean_regret = matrix.mean(axis=0)
    best_mean = float(mean_regret.min())
    tied = mean_regret.index[np.isclose(mean_regret, best_mean, atol=GAP_TOL, rtol=0)]
    sbs = mean_runtime.reindex(tied).sort_values(kind="mergesort").index[0]

    sbs_regret = matrix[sbs].astype(float)
    values = sbs_regret.to_numpy(dtype=float)
    n = int(matrix.shape[0])

    # SBS attainment: SBS belongs to VBS set (regret == 0).
    sbs_attains = float(100.0 * np.mean(sbs_regret <= GAP_TOL))

    stats = {
        "SBS": sbs,
        "# Inst.": n,
        "SBS attains VBS [%]": sbs_attains,
        "Mean regret": float(sbs_regret.mean()),
        "p90 regret": empirical_quantile_higher(sbs_regret, 0.90),
        "Max regret": float(sbs_regret.max()),
    }

    # Threshold exceedance.  For threshold 0, "positive regret" is
    # regret > GAP_TOL (consistent with make_paper_results_risk.py).
    for threshold in REGRET_THRESHOLDS:
        label = f"Share > {threshold:g}"
        stats[label] = float(100.0 * np.mean(values > threshold + GAP_TOL))

    return stats


def make_regret_exceedance_latex(summary: pd.DataFrame) -> str:
    """Render Table B as a LaTeX table."""
    lines = [
        r"\begin{table}[tbp]",
        r"\centering",
        r"\caption{SBS complementarity and regret exceedance. "
        r"``Attains VBS'' is the share of instances on which the SBS belongs "
        r"to the VBS set. Exceedance shares are the percentages of instances "
        r"on which SBS regret exceeds the given threshold. Regret is in "
        r"percent for distance and time objectives and in percentage points "
        r"for the on-time rate.}",
        r"\label{tab:appendix_regret_exceedance}",
        r"\footnotesize",
        r"\setlength{\tabcolsep}{3pt}",
        r"\begin{tabular}{@{}llrrrrrrrrr@{}}",
        r"\toprule",
        r"Instance Set & Objective & \# Inst. & Attains & $>$0 & $>$1 & $>$5 "
        r"& $>$10 & Mean & $p_{90}$ & Max \\",
        r" & & & VBS [\%] & [\%] & [\%] & [\%] & [\%] & [\%] & [\%] & [\%] \\",
        r"\midrule",
    ]

    prev_set: str | None = None
    row_end = r" \\"

    for _, row in summary.iterrows():
        iset = row["Instance Set"]
        obj = row["Objective"]
        first = rf"\textit{{{latex_escape(iset)}}}" if iset != prev_set else ""
        prev_set = iset

        # On-time rate uses pp; mark in the objective label.
        obj_display = latex_escape(obj)
        is_pp = "pp" in obj

        def fmt(v, d=1):
            return f"{v:.{d}f}"

        lines.append(
            " & ".join(
                [
                    first,
                    obj_display,
                    f"{int(row['# Inst.']):,}",
                    fmt(row["SBS attains VBS [%]"]),
                    fmt(row["Share > 0"]),
                    fmt(row["Share > 1"]),
                    fmt(row["Share > 5"]),
                    fmt(row["Share > 10"]),
                    fmt(row["Mean regret"], 2),
                    fmt(row["p90 regret"], 2),
                    fmt(row["Max regret"], 2),
                ]
            )
            + row_end
        )

    lines += [r"\bottomrule", r"\end{tabular}", r"\end{table}", ""]
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Table C: stage-restricted residual regret
# ---------------------------------------------------------------------------

def stage_restricted_residual(
    work: pd.DataFrame,
    stage_col: str,
    sbs_component: str | float,
    *,
    instance_col: str = "instance_name",
) -> float:
    """
    Best achievable mean regret when ``stage_col`` is fixed to
    ``sbs_component``.  Returns NaN if the component is absent or no
    pipeline matches.
    """
    if pd.isna(sbs_component) or str(sbs_component).strip() == "":
        return float("nan")
    subset = work[work[stage_col] == sbs_component]
    if subset.empty:
        return float("nan")
    min_regret = subset.groupby(instance_col)["regret"].min()
    # Guard: must cover all instances.
    n_expected = work[instance_col].nunique()
    if min_regret.shape[0] != n_expected:
        raise ValueError(
            f"Stage restriction on {stage_col}={sbs_component}: "
            f"covers {min_regret.shape[0]} instances, expected {n_expected}."
        )
    return float(min_regret.mean())


def compute_stage_restriction(
    work: pd.DataFrame,
    instance_set_display: str,
    *,
    instance_col: str = "instance_name",
) -> dict:
    """Compute full SBS regret and stage-restricted residuals for one set."""
    matrix = work.pivot(index=instance_col, columns="strategy", values="regret")
    mean_runtime = work.groupby("strategy")["total_cpu_time"].mean()
    mean_regret = matrix.mean(axis=0)
    best_mean = float(mean_regret.min())
    tied = mean_regret.index[np.isclose(mean_regret, best_mean, atol=GAP_TOL, rtol=0)]
    sbs = mean_runtime.reindex(tied).sort_values(kind="mergesort").index[0]

    # SBS components from the algo columns.
    sbs_row = work[work["strategy"] == sbs].iloc[0]
    sbs_ia = sbs_row.get("item_assignment_algo")
    sbs_b = sbs_row.get("batching_algo")
    sbs_r = sbs_row.get("routing_algo")

    full_mean = float(matrix[sbs].mean())
    n = int(matrix.shape[0])

    ia_residual = stage_restricted_residual(work, "item_assignment_algo", sbs_ia)
    b_residual = stage_restricted_residual(work, "batching_algo", sbs_b)
    r_residual = stage_restricted_residual(work, "routing_algo", sbs_r)

    return {
        "Instance Set": instance_set_display,
        "SBS": sbs,
        "Full regret [%]": round(full_mean, 4),
        "IA fixed [%]": round(ia_residual, 4) if not np.isnan(ia_residual) else None,
        "Batching fixed [%]": round(b_residual, 4) if not np.isnan(b_residual) else None,
        "Routing fixed [%]": round(r_residual, 4) if not np.isnan(r_residual) else None,
        "# Inst.": n,
    }


def make_stage_restriction_latex(table: pd.DataFrame) -> str:
    """Render Table C as a LaTeX table."""
    lines = [
        r"\begin{table}[tbp]",
        r"\centering",
        r"\caption{Stage-restricted residual regret for the OBRP instance sets. "
        r"``Full'' is the mean SBS regret. Each ``fixed'' column reports the "
        r"best achievable mean regret after fixing that stage to the SBS "
        r"configuration while allowing all remaining stages to vary. "
        r"Residuals are not additive because stages interact. "
        r"``---'' marks a stage that is absent or inseparable "
        r"(integrated batching-routing).}",
        r"\label{tab:appendix_stage_restriction}",
        r"\small",
        r"\begin{tabular}{@{}llrrrrr@{}}",
        r"\toprule",
        r"Instance Set & SBS & Full & IA fixed & Batch fixed & Route fixed & \# Inst. \\",
        r" & & regret [\%] & [\%] & [\%] & [\%] & \\",
        r"\midrule",
    ]

    def fmt(v):
        return NA_STR if v is None or (isinstance(v, float) and np.isnan(v)) else f"{v:.2f}"

    for _, row in table.iterrows():
        lines.append(
            " & ".join(
                [
                    rf"\textit{{{latex_escape(row['Instance Set'])}}}",
                    latex_escape(row["SBS"]).replace("+", " + "),
                    fmt(row["Full regret [%]"]),
                    fmt(row["IA fixed [%]"]),
                    fmt(row["Batching fixed [%]"]),
                    fmt(row["Routing fixed [%]"]),
                    f"{int(row['# Inst.']):,}",
                ]
            )
            + r" \\"
        )

    lines += [r"\bottomrule", r"\end{tabular}", r"\end{table}", ""]
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------

def analyze_distance_set(
    df: pd.DataFrame, instance_set: str
) -> tuple[list[dict], list[dict], pd.DataFrame, dict | None, int]:
    """
    Analyze one distance-based instance set.

    Returns (exceedance_rows, winner_comp_rows, winner_pipeline_df,
    stage_restriction_row, n_winners).
    """
    display = DISPLAY_INSTANCE_NAMES.get(instance_set, instance_set)
    subset = df[df["instance_set"] == instance_set].copy()
    if "scheduling_algo" in subset.columns:
        subset = subset[is_missing_or_empty(subset["scheduling_algo"])]
    if subset.empty:
        return [], [], pd.DataFrame(), None, 0

    complete = prepare_complete_portfolio(subset, DISTANCE, context=display)
    work = add_instance_regrets(complete, DISTANCE)
    work = add_vbs_membership(work, DISTANCE)
    work = pipeline_winner_credits(work)

    n_winners = count_distinct_winners(work)

    # Table B: complementarity and exceedance.
    comp_stats = sbs_complementarity_stats(work, DISTANCE)
    exceedance_row = {
        "Instance Set": display,
        "Objective": DISTANCE.label,
        "SBS": comp_stats["SBS"],
        "# Inst.": comp_stats["# Inst."],
        "SBS attains VBS [%]": comp_stats["SBS attains VBS [%]"],
        "Share > 0": comp_stats["Share > 0"],
        "Share > 1": comp_stats["Share > 1"],
        "Share > 5": comp_stats["Share > 5"],
        "Share > 10": comp_stats["Share > 10"],
        "Mean regret": comp_stats["Mean regret"],
        "p90 regret": comp_stats["p90 regret"],
        "Max regret": comp_stats["Max regret"],
    }

    # Table A: component-level winner distribution.
    comp_dist = component_winner_distribution(work, display, DISTANCE.label)
    assert_winner_shares_sum(comp_dist, display, DISTANCE.label)

    # Full pipeline-level distribution (CSV only).
    pipe_dist = pipeline_winner_distribution(work, display, DISTANCE.label)

    # Table C: stage restriction (OBRP sets only).
    stage_row = None
    if instance_set in OBRP_SETS:
        stage_row = compute_stage_restriction(work, display)

    return [exceedance_row], [comp_dist], pipe_dist, stage_row, n_winners


def analyze_kris(
    df: pd.DataFrame,
) -> tuple[list[dict], list[dict], pd.DataFrame, list[dict], dict[str, int]]:
    """Analyze the Kris instance set across its three objectives."""
    kris = kris_frame(df, common_instances=True)
    if kris.empty:
        return [], [], pd.DataFrame(), [], {}

    exceedance_rows: list[dict] = []
    comp_rows: list[pd.DataFrame] = []
    pipe_rows: list[pd.DataFrame] = []
    n_winners: dict[str, int] = {}

    for objective in KRIS_OBJECTIVES:
        if objective.column not in kris.columns:
            continue
        complete = prepare_complete_portfolio(kris, objective, context="Kris")
        work = add_instance_regrets(complete, objective)
        work = add_vbs_membership(work, objective)
        work = pipeline_winner_credits(work)

        nw = count_distinct_winners(work)
        n_winners[objective.label] = nw

        comp_stats = sbs_complementarity_stats(work, objective)
        exceedance_rows.append(
            {
                "Instance Set": "Kris",
                "Objective": objective.label,
                "SBS": comp_stats["SBS"],
                "# Inst.": comp_stats["# Inst."],
                "SBS attains VBS [%]": comp_stats["SBS attains VBS [%]"],
                "Share > 0": comp_stats["Share > 0"],
                "Share > 1": comp_stats["Share > 1"],
                "Share > 5": comp_stats["Share > 5"],
                "Share > 10": comp_stats["Share > 10"],
                "Mean regret": comp_stats["Mean regret"],
                "p90 regret": comp_stats["p90 regret"],
                "Max regret": comp_stats["Max regret"],
            }
        )

        comp_dist = component_winner_distribution(work, "Kris", objective.label)
        assert_winner_shares_sum(comp_dist, "Kris", objective.label)
        comp_rows.append(comp_dist)
        pipe_rows.append(pipeline_winner_distribution(work, "Kris", objective.label))

    pipe_df = pd.concat(pipe_rows, ignore_index=True) if pipe_rows else pd.DataFrame()
    return exceedance_rows, comp_rows, pipe_df, [], n_winners


def build_all_tables(df: pd.DataFrame) -> dict:
    """
    Build all three appendix tables plus the # Winners summary.

    Returns a dict with keys:
        exceedance      DataFrame (Table B)
        winners_comp    DataFrame (Table A, component level)
        winners_pipe    DataFrame (pipeline level, CSV only)
        stage           DataFrame (Table C)
        n_winners       dict {display_name: int}
    """
    all_exceedance: list[dict] = []
    all_comp: list[pd.DataFrame] = []
    all_pipe: list[pd.DataFrame] = []
    all_stage: list[dict] = []
    n_winners: dict[str, int] = {}

    for instance_set in DISTANCE_SETS:
        exc, comp, pipe, stage, nw = analyze_distance_set(df, instance_set)
        all_exceedance.extend(exc)
        if comp:
            all_comp.extend(comp)
        if not pipe.empty:
            all_pipe.append(pipe)
        if stage is not None:
            all_stage.append(stage)
        if exc:
            n_winners[exc[0]["Instance Set"]] = nw

    kris_exc, kris_comp, kris_pipe, kris_stage, kris_nw = analyze_kris(df)
    all_exceedance.extend(kris_exc)
    all_comp.extend(kris_comp)
    if not kris_pipe.empty:
        all_pipe.append(kris_pipe)
    n_winners.update(kris_nw)

    exceedance_df = pd.DataFrame(all_exceedance)

    # Order by INSTANCE_ORDER.
    order = {DISPLAY_INSTANCE_NAMES.get(n, n): i for i, n in enumerate(INSTANCE_ORDER)}
    exceedance_df["_order"] = exceedance_df["Instance Set"].map(order)
    exceedance_df = exceedance_df.sort_values("_order").drop(columns="_order").reset_index(drop=True)

    winners_comp_df = pd.concat(all_comp, ignore_index=True) if all_comp else pd.DataFrame()
    winners_pipe_df = pd.concat(all_pipe, ignore_index=True) if all_pipe else pd.DataFrame()

    stage_df = pd.DataFrame(all_stage)

    return {
        "exceedance": exceedance_df,
        "winners_comp": winners_comp_df,
        "winners_pipe": winners_pipe_df,
        "stage": stage_df,
        "n_winners": n_winners,
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate portfolio-diversity appendix tables."
    )
    parser.add_argument(
        "--df-results",
        type=Path,
        default=Path(__file__).resolve().parent / "df_results.pkl",
    )
    parser.add_argument(
        "--tables-dir",
        type=Path,
        default=Path(__file__).resolve().parent / "tables",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    if not args.df_results.exists():
        raise FileNotFoundError(f"Result file not found: {args.df_results}")

    args.tables_dir.mkdir(parents=True, exist_ok=True)

    raw = pd.read_pickle(args.df_results)
    df = prepare_df_results(raw)

    results = build_all_tables(df)

    exceedance = results["exceedance"]
    winners_comp = results["winners_comp"]
    winners_pipe = results["winners_pipe"]
    stage = results["stage"]
    n_winners = results["n_winners"]

    # --- Assertions ---
    # Instance counts must match the retained analysis population.
    expected_counts = {
        "SPRP": 2400,
        "SPRP-SS": 14300,
        "BahceciOencan": 1350,
        "HennWaescher": 5759,
        "MuterOencan": 270,
        "Foodmart": 144,
        "Kris": 479,
    }
    for _, row in exceedance.iterrows():
        iset = row["Instance Set"]
        n = int(row["# Inst."])
        expected = expected_counts.get(iset)
        if expected is not None and n != expected:
            raise AssertionError(
                f"{iset}/{row['Objective']}: # Inst. = {n}, expected {expected}"
            )

    # Winner shares must sum to 100% per (instance set, objective, stage).
    for (iset, obj), grp in winners_comp.groupby(["Instance Set", "Objective"]):
        for stage_label, stage_grp in grp.groupby("Stage"):
            total = stage_grp["Winner Share [%]"].sum()
            if abs(total - 100.0) > SUM_TOL:
                raise AssertionError(
                    f"{iset}/{obj}/{stage_label}: shares sum to {total:.4f}, expected 100.0"
                )

    # --- Write outputs ---
    exceedance.to_csv(args.tables_dir / "appendix_regret_exceedance.csv", index=False)
    (args.tables_dir / "appendix_regret_exceedance.tex").write_text(
        make_regret_exceedance_latex(exceedance), encoding="utf-8"
    )

    winners_comp.to_csv(args.tables_dir / "appendix_winner_distribution.csv", index=False)
    (args.tables_dir / "appendix_winner_distribution.tex").write_text(
        make_winner_distribution_latex(winners_comp), encoding="utf-8"
    )

    winners_pipe.to_csv(args.tables_dir / "appendix_winner_pipelines.csv", index=False)

    stage.to_csv(args.tables_dir / "appendix_stage_restriction.csv", index=False)
    (args.tables_dir / "appendix_stage_restriction.tex").write_text(
        make_stage_restriction_latex(stage), encoding="utf-8"
    )

    # --- Report ---
    print("\n" + "=" * 72)
    print("Portfolio-diversity diagnostics")
    print("=" * 72)

    print("\n# Winners (tie-aware, complete-portfolio):")
    for iset in ["SPRP", "SPRP-SS", "BahceciOencan", "HennWaescher",
                 "MuterOencan", "Foodmart"]:
        n = n_winners.get(iset, "?")
        print(f"  {iset:20s}: {n}")
    for obj_label in ["total picking time", "makespan", "on-time rate (pp)"]:
        n = n_winners.get(obj_label, "?")
        print(f"  Kris/{obj_label:20s}: {n}")

    print("\nSBS complementarity and regret exceedance:")
    show_cols = [
        "Instance Set", "Objective", "# Inst.", "SBS attains VBS [%]",
        "Share > 0", "Share > 1", "Share > 5", "Share > 10",
        "Mean regret", "p90 regret", "Max regret",
    ]
    print(exceedance[show_cols].round(2).to_string(index=False))

    print("\nStage-restricted residual regret:")
    print(stage.round(4).to_string(index=False))

    print(f"\nGenerated tables in {args.tables_dir}")
    print("  appendix_winner_distribution.tex / .csv  (Table A)")
    print("  appendix_winner_pipelines.csv            (pipeline detail)")
    print("  appendix_regret_exceedance.tex / .csv     (Table B)")
    print("  appendix_stage_restriction.tex / .csv     (Table C)")


if __name__ == "__main__":
    main()
