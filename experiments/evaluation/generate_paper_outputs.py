"""
Single entry point for generating all paper and appendix tables from the
unified portfolio.

Uses the consolidated modules (data, portfolio, metrics, components) so that
every output derives from the same definitions. No RR-NF exclusion; no
parallel portfolio.

Outputs (in experiments/evaluation/tables/):
    pipeline_space.{tex,csv}          pipeline-space table (Section 5.2)
    validation.{tex,csv}              literature comparison (Section 5.3)
    selection.{tex,csv}                SBS regret + # Winners (Section 5.4)
    appendix_winner_distribution.{tex,csv}   Table A
    appendix_winner_pipelines.csv            pipeline-level detail
    appendix_regret_exceedance.{tex,csv}     Table B
    appendix_stage_restriction.{tex,csv}     Table C
    loader_runtimes.{tex,csv}               loader timing + cache impact
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))

from data import (
    DISPLAY_INSTANCE_NAMES,
    DISTANCE,
    DISTANCE_SETS,
    INSTANCE_ORDER,
    KRIS_OBJECTIVES,
    REGRET_THRESHOLDS,
    is_missing_or_empty,
    kris_frame,
    latex_escape,
    prepare_df_results,
    to_numeric_nonempty,
)
from portfolio import (
    add_structure,
    compute_pipeline_results_overview,
    prepare_complete_portfolio,
)
from metrics import (
    add_instance_regrets,
    add_vbs_membership,
    count_distinct_winners,
    gap_min,
    pipeline_winner_credits,
    reference_from_solvers,
    reference_type_from_bounds,
    reference_type_from_lb_ub,
    regret_matrix,
    selection_stats,
    sbs_attainment_share,
    positive_regret_share,
    threshold_column,
)
from components import (
    compute_component_restriction,
    component_winner_distribution,
)


# ===========================================================================
# Config
# ===========================================================================

TABLES_DIR = Path(__file__).resolve().parent / "tables"
PAPER_TABLES_DIR = Path(__file__).resolve().parents[2] / "paper_agent" / "tables"
CACHE_PATH = Path(__file__).resolve().parent / "df_results.pkl"
REF_DIR = Path(__file__).resolve().parents[2] / "data" / "results"
RUNTIME_DIR = Path(__file__).resolve().parents[1] / "output" / "runtimes"

OBRP_SETS = ["BahceciOencan", "HennWaescher", "MuterOencan", "FoodmartData"]
COMPONENT_RESTRICTION_SETS = OBRP_SETS + ["SPRP-SS"]

# Tables that are \input'd by the manuscript and must be copied to paper_agent/tables/.
APPENDIX_TABLES = [
    "appendix_winner_distribution.tex",
    "appendix_regret_exceedance.tex",
    "appendix_stage_restriction.tex",
]

NA_STR = "---"


def write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def write_csv(path: Path, df: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False, encoding="utf-8")


def copy_to_paper_tables() -> None:
    r"""Copy \input'd appendix tables to paper_agent/tables/."""
    PAPER_TABLES_DIR.mkdir(parents=True, exist_ok=True)
    for name in APPENDIX_TABLES:
        src = TABLES_DIR / name
        if src.exists():
            dst = PAPER_TABLES_DIR / name
            dst.write_text(src.read_text(encoding="utf-8"), encoding="utf-8")
            print(f"  Copied {name} -> {dst}")


def write_csv(path: Path, df: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False, encoding="utf-8")


def _read_result_csv(path: Path, skiprows=None) -> pd.DataFrame:
    return pd.read_csv(path, sep=";", decimal=",", thousands=".", skiprows=skiprows)


# ===========================================================================
# Table 1: pipeline space
# ===========================================================================

def generate_pipeline_space(df: pd.DataFrame) -> pd.DataFrame:
    overview = compute_pipeline_results_overview(df)
    pro = overview.reindex(INSTANCE_ORDER)

    lines = [
        r"\begin{table}[tbp]",
        r"\centering",
        r"\caption{Full applicable pipeline space and evaluated benchmark portfolio.}",
        r"\label{tab:pipeline_results}",
        r"\small",
        r"\setlength{\tabcolsep}{4pt}",
        r"\begin{tabular}{@{}lrrrrrrrr@{}}",
        r"\toprule",
        r"& \multicolumn{6}{c}{Full applicable configurations}",
        r"& \multicolumn{2}{c}{Pipelines per instance} \\",
        r"\cmidrule(lr){2-7}",
        r"\cmidrule(lr){8-9}",
        r"Instance Set"
        r" & $|\mathit{IA}^*|$"
        r" & $|\mathit{B}^*|$"
        r" & $|\mathit{R}^*|$"
        r" & $|\mathit{IAR}^*|$"
        r" & $|\mathit{BR}^*|$"
        r" & $|\mathit{S}^*|$"
        r" & Full & Evaluated \\",
        r"\midrule",
    ]

    for idx, row in pro.iterrows():
        name = DISPLAY_INSTANCE_NAMES.get(idx, idx)
        vals = " & ".join(str(int(v)) if pd.notna(v) else "--" for v in row)
        lines.append(rf"\textit{{{name}}} & {vals} \\")

    lines += [
        r"\midrule",
        rf"$\sum$ &  &  &  &  &  & {int(pro['n_instances'].sum()):,} "
        rf"& {int(pro['n_pipelines'].sum()):,}\\",
        r"\bottomrule",
        r"\end{tabular}",
        r"\end{table}",
        "",
    ]
    write_text(TABLES_DIR / "pipeline_space.tex", "\n".join(lines))
    write_csv(TABLES_DIR / "pipeline_space.csv", overview.reset_index())
    return overview


# ===========================================================================
# Table 2: validation (literature comparison)
# ===========================================================================

def _validation_row(vbs_df: pd.DataFrame, set_label: str, objective: str,
                    n_feasible: int | None = None) -> dict:
    ref = vbs_df[vbs_df["reference_value"].notna()].copy()
    is_opt = ref["reference_type"] == "optimum"
    if n_feasible is None:
        n_feasible = int(vbs_df["instance_name"].nunique())
    return {
        "Instance Set": set_label,
        "Objective": objective,
        "n_feasible": n_feasible,
        "n_referenced": int(ref["instance_name"].nunique()),
        "n_proven_opt": int(is_opt.sum()),
        "gap_to_ref_[%]": float(ref["gap_[%]"].mean()),
        "gap_to_opt_[%]": float(ref.loc[is_opt, "gap_[%]"].mean()) if is_opt.any() else float("nan"),
    }


def generate_validation(df: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict] = []

    # Apply the same degenerate-row filter as bks_evaluation.py: total_distance
    # defaults to 0 when routing did not run, and 0 would spuriously win the
    # distance VBS.
    valid = df[to_numeric_nonempty(df["total_distance"]) > 0].copy()

    # Compute n_feasible per instance set from the valid-distance frame.
    feasible_counts: dict[str, int] = {}
    for raw, display in [("SPRP","SPRP"),("SPRP-SS","SPRP-SS"),
                         ("BahceciOencan","BahceciOencan"),
                         ("HennWaescher","HennWaescher"),
                         ("MuterOencan","MuterOencan"),
                         ("FoodmartData","Foodmart")]:
        feasible_counts[display] = int(
            valid[valid["instance_set"] == raw]["instance_name"].nunique()
        )

    # SPRP
    ref_sprp = _read_result_csv(REF_DIR / "results_SPRP.csv")
    ref_sprp["filename"] = ref_sprp.apply(
        lambda r: f"unit_F1_m{r['num aisles']}_C{r['num cells']}_a{r['num articles']}_{r['random seed']}",
        axis=1,
    )
    ref_sprp = reference_from_solvers(ref_sprp, ["GS MIP cost", "Netw MIP cost", "DP cost"])
    df_sprp = valid[valid["instance_set"] == "SPRP"].copy()
    df_sprp = df_sprp.merge(
        ref_sprp[["filename", "reference_value", "reference_type"]],
        left_on="instance_name", right_on="filename", how="left",
    )
    df_sprp["gap_[%]"] = gap_min(df_sprp, "total_distance")
    vbs_sprp = df_sprp.sort_values(["total_distance", "total_cpu_time"]).groupby("instance_name").first().reset_index()
    rows.append(_validation_row(vbs_sprp, "SPRP", "distance", feasible_counts["SPRP"]))

    # SPRP-SS
    ref_ss = _read_result_csv(REF_DIR / "results_SPRP-SS.csv")
    ref_ss["demand_helper"] = np.where(ref_ss["unit demand"], "unit", "varying")
    ref_ss["filename"] = ref_ss.apply(
        lambda r: f"{r['demand_helper']}_F{r['alpha']}_m{r['num aisles']}_C{r['num cells']}_a{r['num articles']}_{r['random seed']}",
        axis=1,
    )
    ref_ss = reference_from_solvers(ref_ss, ["GS MIP cost", "Netw MIP cost"])
    df_ss = valid[valid["instance_set"] == "SPRP-SS"].copy()
    df_ss = df_ss.merge(
        ref_ss[["filename", "reference_value", "reference_type"]],
        left_on="instance_name", right_on="filename", how="left",
    )
    df_ss["gap_[%]"] = gap_min(df_ss, "total_distance")
    vbs_ss = df_ss.sort_values(["total_distance", "total_cpu_time"]).groupby("instance_name").first().reset_index()
    rows.append(_validation_row(vbs_ss, "SPRP-SS", "distance", feasible_counts["SPRP-SS"]))

    # BahceciOencan
    ref_bo = _read_result_csv(REF_DIR / "results_BahceciOencan.csv")
    ref_bo["filename"] = ref_bo["filename"].str.removesuffix(".txt")
    ref_bo = ref_bo[ref_bo["policy"] == "optimal"].copy()
    ref_bo["reference_value"] = ref_bo["UB"]
    ref_bo["reference_type"] = reference_type_from_bounds(ref_bo)
    df_bo = valid[valid["instance_set"] == "BahceciOencan"].copy()
    df_bo = df_bo.merge(
        ref_bo[["filename", "reference_value", "reference_type"]],
        left_on="instance_name", right_on="filename", how="left",
    )
    df_bo["gap_[%]"] = gap_min(df_bo, "total_distance")
    vbs_bo = df_bo.sort_values(["total_distance", "total_cpu_time"]).groupby("instance_name").first().reset_index()
    rows.append(_validation_row(vbs_bo, "BahceciOencan", "distance", feasible_counts["BahceciOencan"]))

    # HennWaescher
    ref_hw = _read_result_csv(REF_DIR / "results_HennWaescher.csv")
    ref_hw["storage_policy"] = ref_hw["filename"].str.split("\\").str[0].str.split("_").str[1]
    ref_hw["filename"] = ref_hw["filename"].str.split("\\").str[-1].str.replace(".txt", "", regex=False)
    ref_hw.loc[ref_hw["storage_policy"] == "uniform", "filename"] = ref_hw.loc[ref_hw["storage_policy"] == "uniform", "filename"] + "_u"
    ref_hw.loc[ref_hw["storage_policy"] == "class-based", "filename"] = ref_hw.loc[ref_hw["storage_policy"] == "class-based", "filename"] + "_cb"
    ref_hw = ref_hw[ref_hw["policy"] == "optimal"].copy()
    ref_hw["reference_value"] = ref_hw["UB"]
    ref_hw["reference_type"] = reference_type_from_bounds(ref_hw)
    df_hw = valid[valid["instance_set"] == "HennWaescher"].copy()
    df_hw = df_hw.merge(
        ref_hw[["filename", "reference_value", "reference_type"]],
        left_on="instance_name", right_on="filename", how="left",
    )
    df_hw = df_hw.dropna(subset=["reference_value"])
    df_hw["gap_[%]"] = gap_min(df_hw, "total_distance")
    vbs_hw = df_hw.sort_values(["total_distance", "total_cpu_time"]).groupby("instance_name").first().reset_index()
    rows.append(_validation_row(vbs_hw, "HennWaescher", "distance", feasible_counts["HennWaescher"]))

    # MuterOencan (two header rows -> skiprows=1; seed shifted by -1)
    ref_mo = _read_result_csv(REF_DIR / "results_Muter.csv", skiprows=1)
    ref_mo["random seed"] = ref_mo["random seed"] - 1
    ref_mo = ref_mo.dropna(subset=["number of orders", "capacity", "random seed"]).copy()
    ref_mo["filename"] = ref_mo.apply(
        lambda r: f"{int(r['number of orders'])}_{int(r['capacity'])}_{int(r['random seed'])}",
        axis=1,
    )
    ref_mo = ref_mo[ref_mo["policy"] == "optimal"].copy()
    ref_mo["reference_value"] = ref_mo["UB"]
    ref_mo["reference_type"] = reference_type_from_bounds(ref_mo)
    df_mo = valid[valid["instance_set"] == "MuterOencan"].copy()
    df_mo = df_mo.merge(
        ref_mo[["filename", "reference_value", "reference_type"]],
        left_on="instance_name", right_on="filename", how="left",
    )
    df_mo["gap_[%]"] = gap_min(df_mo, "total_distance")
    vbs_mo = df_mo.sort_values(["total_distance", "total_cpu_time"]).groupby("instance_name").first().reset_index()
    rows.append(_validation_row(vbs_mo, "MuterOencan", "distance", feasible_counts["MuterOencan"]))

    # Foodmart
    ref_fm = _read_result_csv(REF_DIR / "results_Foodmart.csv")
    ref_fm["reference_value"] = ref_fm["UB"]
    ref_fm["reference_type"] = reference_type_from_lb_ub(ref_fm)
    df_fm = valid[valid["instance_set"] == "FoodmartData"].copy()
    df_fm["instance_name"] = df_fm["instance_name"].str.replace(r"^instances_|_MAL$", "", regex=True)
    df_fm = df_fm.merge(
        ref_fm[["Name", "reference_value", "reference_type"]],
        left_on="instance_name", right_on="Name", how="left",
    )
    df_fm["gap_[%]"] = gap_min(df_fm, "total_distance")
    vbs_fm = df_fm.loc[df_fm.groupby("instance_name")["total_distance"].idxmin()].copy()
    rows.append(_validation_row(vbs_fm, "Foodmart", "distance", feasible_counts["Foodmart"]))

    # Kris (picking time)
    rows.append(_generate_kris_validation(df))

    summary = pd.DataFrame(rows)

    lines = [
        r"\begin{table}[tbp]",
        r"\centering",
        r"\caption{Comparison against reference solutions. Gaps are means of",
        r"per-instance relative gaps.}",
        r"\label{tab:validation}",
        r"\small",
        r"\setlength{\tabcolsep}{6pt}",
        r"\begin{tabular}{llrrrrr}",
        r"\toprule",
        r"Instance Set & Objective & \# Feasible & \# Referenced & \# Proven Opt."
        r" & Gap to Ref. [\%] & Gap to Opt. [\%] \\",
        r"\midrule",
    ]
    for _, row in summary.iterrows():
        lines.append(
            rf"\textit{{{latex_escape(row['Instance Set'])}}} "
            rf"& {latex_escape(row['Objective'])} "
            rf"& {int(row['n_feasible']):,} "
            rf"& {int(row['n_referenced']):,} "
            rf"& {int(row['n_proven_opt']):,} "
            rf"& {row['gap_to_ref_[%]']:.3f} "
            rf"& {row['gap_to_opt_[%]']:.3f}" + r" \\"
        )
    lines += [r"\bottomrule", r"\end{tabular}", r"\end{table}", ""]
    write_text(TABLES_DIR / "validation.tex", "\n".join(lines))
    write_csv(TABLES_DIR / "validation.csv", summary)
    return summary


def _generate_kris_validation(df: pd.DataFrame) -> dict:
    from kris_evaluation import build_kris_reference, build_kris_vbs, build_kris_table

    kris_ref_dir = REF_DIR / "briant_results"
    small_dir = REF_DIR / "allSolutions" / "solutionssmall"
    large_dir = REF_DIR / "allSolutions" / "solutionslarge"

    reference = build_kris_reference(small_dir, large_dir, kris_ref_dir)
    if reference is None or reference.empty:
        return {
            "Instance Set": "Kris", "Objective": "picking time",
            "n_feasible": 0, "n_referenced": 0, "n_proven_opt": 0,
            "gap_to_ref_[%]": float("nan"), "gap_to_opt_[%]": float("nan"),
        }

    result = build_kris_vbs(df, reference)
    if result is None:
        return {
            "Instance Set": "Kris", "Objective": "picking time",
            "n_feasible": 0, "n_referenced": 0, "n_proven_opt": 0,
            "gap_to_ref_[%]": float("nan"), "gap_to_opt_[%]": float("nan"),
        }
    vbs, kris = result

    n_evaluated = df[df["instance_set"] == "Kris"]["instance_name"].nunique()
    table = build_kris_table(vbs, n_evaluated)
    row = table.iloc[0]

    return {
        "Instance Set": "Kris",
        "Objective": "picking time",
        "n_feasible": int(row["n_feasible"]),
        "n_referenced": int(row["n_referenced"]),
        "n_proven_opt": int(row["n_proven_opt"]),
        "gap_to_ref_[%]": float(row["gap_to_ref_[%]"]),
        "gap_to_opt_[%]": float(row["gap_to_opt_[%]"]),
    }


# ===========================================================================
# Table 3: selection (SBS regret + # Winners)
# ===========================================================================

def generate_selection(df: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict] = []

    for instance_set in DISTANCE_SETS:
        subset = df[df["instance_set"] == instance_set].copy()
        if subset.empty:
            continue
        if "scheduling_algo" in subset.columns:
            subset = subset[is_missing_or_empty(subset["scheduling_algo"])]
        if subset.empty:
            continue

        display = DISPLAY_INSTANCE_NAMES.get(instance_set, instance_set)
        complete = prepare_complete_portfolio(subset, DISTANCE, context=display)
        work = add_instance_regrets(complete, DISTANCE)
        work = add_vbs_membership(work)
        matrix = regret_matrix(work)
        mean_runtime = work.groupby("strategy")["total_cpu_time"].mean()
        stats = selection_stats(matrix, mean_runtime)
        n_winners = count_distinct_winners(work)
        sbs_at_vbs = sbs_attainment_share(work, stats["SBS"])

        rows.append({
            "Instance Set": display,
            "Objective": DISTANCE.label,
            "SBS": stats["SBS"],
            "Mean regret": stats["Mean regret"],
            "p90 regret": stats["p90 regret"],
            "Max regret": stats["Max regret"],
            "SBS at VBS [%]": round(sbs_at_vbs, 1),
            "# Winners": n_winners,
            "# Inst.": stats["# Inst."],
            "# Pipe.": stats["# Pipe."],
        })

    kris = kris_frame(df, common_instances=True)
    if not kris.empty:
        for objective in KRIS_OBJECTIVES:
            if objective.column not in kris.columns:
                continue
            complete = prepare_complete_portfolio(kris, objective, context="Kris")
            work = add_instance_regrets(complete, objective)
            work = add_vbs_membership(work)
            matrix = regret_matrix(work)
            mean_runtime = work.groupby("strategy")["total_cpu_time"].mean()
            stats = selection_stats(matrix, mean_runtime)
            n_winners = count_distinct_winners(work)
            sbs_at_vbs = sbs_attainment_share(work, stats["SBS"])

            rows.append({
                "Instance Set": "Kris",
                "Objective": objective.label,
                "SBS": stats["SBS"],
                "Mean regret": stats["Mean regret"],
                "p90 regret": stats["p90 regret"],
                "Max regret": stats["Max regret"],
                "SBS at VBS [%]": round(sbs_at_vbs, 1),
                "# Winners": n_winners,
                "# Inst.": stats["# Inst."],
                "# Pipe.": stats["# Pipe."],
            })

    summary = pd.DataFrame(rows)

    order = {DISPLAY_INSTANCE_NAMES.get(n, n): i for i, n in enumerate(INSTANCE_ORDER)}
    obj_order = {"distance": 0, "total picking time": 1, "makespan": 2, "on-time rate (pp)": 3}
    summary["_set_order"] = summary["Instance Set"].map(order)
    summary["_obj_order"] = summary["Objective"].map(obj_order)
    summary = summary.sort_values(["_set_order", "_obj_order"], kind="mergesort").drop(columns=["_set_order", "_obj_order"]).reset_index(drop=True)

    lines = [
        r"\begin{table}[!t]",
        r"\centering",
        r"\caption{Instance-wise regret of the SBS relative to the oracle VBS",
        r"and the SBS attainment rate (share of instances where the SBS",
        r"belongs to the VBS set) per instance set and objective.}",
        r"\label{tab:selection}",
        r"\small",
        r"\setlength{\tabcolsep}{5pt}",
        r"\begin{tabular}{@{}lllrrrr@{}}",
        r"\toprule",
        r"Instance Set & Objective & SBS & Mean & $p_{90}$ & Max & SBS@VBS \\",
        r"\midrule",
    ]
    prev_set: str | None = None
    for _, row in summary.iterrows():
        first = rf"\textit{{{latex_escape(row['Instance Set'])}}}" if row["Instance Set"] != prev_set else ""
        prev_set = row["Instance Set"]
        lines.append(
            f"{first} & {latex_escape(row['Objective'])} "
            f"& {latex_escape(row['SBS']).replace('+', ' + ')} "
            f"& {row['Mean regret']:.2f} & {row['p90 regret']:.2f} "
            f"& {row['Max regret']:.2f} & {row['SBS at VBS [%]']:.0f}" + r" \\"
        )
    lines += [r"\bottomrule", r"\end{tabular}", r"\end{table}", ""]
    write_text(TABLES_DIR / "selection.tex", "\n".join(lines))
    write_csv(TABLES_DIR / "selection.csv", summary)
    return summary


# ===========================================================================
# Appendix Table A: winner distribution
# ===========================================================================

def generate_winner_distribution(df: pd.DataFrame) -> pd.DataFrame:
    all_rows: list[pd.DataFrame] = []
    pipeline_rows: list[dict] = []

    def _process(subset, display, objective):
        complete = prepare_complete_portfolio(subset, objective, context=display)
        work = add_instance_regrets(complete, objective)
        work = add_vbs_membership(work)
        work = pipeline_winner_credits(work)

        comp_dist = component_winner_distribution(work, display, objective.label)
        if not comp_dist.empty:
            all_rows.append(comp_dist)

        pipe_credits = work[work["in_vbs"]].groupby("strategy")["winner_credit"].sum()
        n_inst = work["instance_name"].nunique()
        for strategy, credit in pipe_credits.items():
            pipeline_rows.append({
                "Instance Set": display,
                "Objective": objective.label,
                "Strategy": strategy,
                "Winner share [%]": round(float(credit) / n_inst * 100, 2),
            })

    for instance_set in DISTANCE_SETS:
        subset = df[df["instance_set"] == instance_set].copy()
        if subset.empty:
            continue
        if "scheduling_algo" in subset.columns:
            subset = subset[is_missing_or_empty(subset["scheduling_algo"])]
        if subset.empty:
            continue
        display = DISPLAY_INSTANCE_NAMES.get(instance_set, instance_set)
        _process(subset, display, DISTANCE)

    kris = kris_frame(df, common_instances=True)
    if not kris.empty:
        for objective in KRIS_OBJECTIVES:
            if objective.column not in kris.columns:
                continue
            _process(kris, "Kris", objective)

    table = pd.concat(all_rows, ignore_index=True) if all_rows else pd.DataFrame()
    pipelines = pd.DataFrame(pipeline_rows)

    if not table.empty:
        sort_map = {DISPLAY_INSTANCE_NAMES.get(n, n): i for i, n in enumerate(INSTANCE_ORDER)}
        table["_order"] = table["Instance Set"].map(sort_map)
        comp_order = {"IA": 0, "Batching": 1, "Routing": 2, "IAR": 3, "BR": 4, "Scheduling": 5}
        table["_comp_order"] = table["Component Type"].map(comp_order)
        table = table.sort_values(["_order", "_comp_order", "Winner share [%]"], ascending=[True, True, False]).drop(columns=["_order", "_comp_order"])

    write_csv(TABLES_DIR / "appendix_winner_distribution.csv", table)
    write_csv(TABLES_DIR / "appendix_winner_pipelines.csv", pipelines)
    _write_winner_distribution_latex(table)
    return table


def _write_winner_distribution_latex(table: pd.DataFrame) -> None:
    lines = [
        r"\begin{longtable}{@{}llllr@{}}",
        r"\caption{Component-level VBS winner distribution with fractional tie handling.}",
        r"\label{tab:appendix_winner_distribution} \\",
        r"\toprule",
        r"Instance Set & Objective & Component Type & Component & Winner share [\%] \\",
        r"\midrule",
        r"\endfirsthead",
        r"\toprule",
        r"Instance Set & Objective & Component Type & Component & Winner share [\%] \\",
        r"\midrule",
        r"\endhead",
        r"\midrule",
        r"\multicolumn{5}{r}{\continuednextpage} \\",
        r"\endfoot",
        r"\bottomrule",
        r"\endlastfoot",
    ]
    for _, row in table.iterrows():
        share_val = row['Winner share [%]']
        lines.append(
            rf"\textit{{{latex_escape(row['Instance Set'])}}} "
            rf"& {latex_escape(row['Objective'])} "
            rf"& {latex_escape(row['Component Type'])} "
            rf"& {latex_escape(row['Component'])} "
            f"& {share_val:.2f}" + r" \\"
        )
    lines.append(r"\end{longtable}")
    write_text(TABLES_DIR / "appendix_winner_distribution.tex", "\n".join(lines))


# ===========================================================================
# Appendix Table B: regret exceedance
# ===========================================================================

def generate_regret_exceedance(df: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict] = []

    def _process(subset, display, objective):
        complete = prepare_complete_portfolio(subset, objective, context=display)
        work = add_instance_regrets(complete, objective)
        matrix = regret_matrix(work)
        mean_runtime = work.groupby("strategy")["total_cpu_time"].mean()
        stats = selection_stats(matrix, mean_runtime)

        row = {
            "Instance Set": display,
            "Objective": objective.label,
            "SBS": stats["SBS"],
            "Mean regret [%]": round(stats["Mean regret"], 4),
            "Max regret [%]": round(stats["Max regret"], 4),
            "SBS at VBS [%]": round(sbs_attainment_share(work, stats["SBS"]), 2),
            "Positive regret [%]": round(positive_regret_share(work, stats["SBS"]), 2),
        }
        for t in REGRET_THRESHOLDS:
            row[threshold_column(t)] = round(stats[threshold_column(t)], 2)
        rows.append(row)

    for instance_set in DISTANCE_SETS:
        subset = df[df["instance_set"] == instance_set].copy()
        if subset.empty:
            continue
        if "scheduling_algo" in subset.columns:
            subset = subset[is_missing_or_empty(subset["scheduling_algo"])]
        if subset.empty:
            continue
        display = DISPLAY_INSTANCE_NAMES.get(instance_set, instance_set)
        _process(subset, display, DISTANCE)

    kris = kris_frame(df, common_instances=True)
    if not kris.empty:
        for objective in KRIS_OBJECTIVES:
            if objective.column not in kris.columns:
                continue
            _process(kris, "Kris", objective)

    summary = pd.DataFrame(rows)

    order = {DISPLAY_INSTANCE_NAMES.get(n, n): i for i, n in enumerate(INSTANCE_ORDER)}
    obj_order = {"distance": 0, "total picking time": 1, "makespan": 2, "on-time rate (pp)": 3}
    summary["_set_order"] = summary["Instance Set"].map(order)
    summary["_obj_order"] = summary["Objective"].map(obj_order)
    summary = summary.sort_values(["_set_order", "_obj_order"], kind="mergesort").drop(columns=["_set_order", "_obj_order"]).reset_index(drop=True)

    write_csv(TABLES_DIR / "appendix_regret_exceedance.csv", summary)
    _write_regret_exceedance_latex(summary)
    return summary


def _write_regret_exceedance_latex(summary: pd.DataFrame) -> None:
    thresh_cols = [threshold_column(t) for t in REGRET_THRESHOLDS]
    n_cols = 7 + len(thresh_cols)
    col_spec = "lll" + "r" * (4 + len(thresh_cols))
    lines = [
        r"\begin{longtable}{@{}" + col_spec + r"@{}}",
        r"\caption{SBS attainment and regret exceedance rates.}",
        r"\label{tab:appendix_regret_exceedance} \\",
        r"\toprule",
        r"Set & Objective & SBS & Mean & Max & SBS@VBS & Pos. "
        + " & ".join(rf"$>{t:g}\%$" for t in REGRET_THRESHOLDS)
        + r" \\",
        r"\midrule",
        r"\endfirsthead",
        r"\toprule",
        r"Set & Objective & SBS & Mean & Max & SBS@VBS & Pos. "
        + " & ".join(rf"$>{t:g}\%$" for t in REGRET_THRESHOLDS)
        + r" \\",
        r"\midrule",
        r"\endhead",
        r"\midrule \multicolumn{" + str(n_cols) + r"}{r}{\continuednextpage} \\",
        r"\endfoot",
        r"\bottomrule \endlastfoot",
    ]
    prev_set: str | None = None
    for _, row in summary.iterrows():
        first = rf"\textit{{{latex_escape(row['Instance Set'])}}}" if row["Instance Set"] != prev_set else ""
        prev_set = row["Instance Set"]
        cells = [
            first,
            latex_escape(row["Objective"]),
            latex_escape(str(row["SBS"])).replace("+", " + "),
            f"{row['Mean regret [%]']:.2f}",
            f"{row['Max regret [%]']:.2f}",
            f"{row['SBS at VBS [%]']:.1f}",
            f"{row['Positive regret [%]']:.1f}",
        ]
        for tc in thresh_cols:
            cells.append(f"{row[tc]:.1f}")
        lines.append(" & ".join(cells) + r" \\")
    lines.append(r"\end{longtable}")
    write_text(TABLES_DIR / "appendix_regret_exceedance.tex", "\n".join(lines))


# ===========================================================================
# Appendix Table C: component-restricted residual regret
# ===========================================================================

def generate_component_restriction(df: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict] = []

    for instance_set in COMPONENT_RESTRICTION_SETS:
        subset = df[df["instance_set"] == instance_set].copy()
        if subset.empty:
            continue
        if "scheduling_algo" in subset.columns:
            subset = subset[is_missing_or_empty(subset["scheduling_algo"])]
        if subset.empty:
            continue

        display = DISPLAY_INSTANCE_NAMES.get(instance_set, instance_set)
        complete = prepare_complete_portfolio(subset, DISTANCE, context=display)
        work = add_instance_regrets(complete, DISTANCE)
        work = add_structure(work)
        row = compute_component_restriction(work, display)
        rows.append(row)

    table = pd.DataFrame(rows)

    comp_cols = ["IA fixed [%]", "Batching fixed [%]", "Routing fixed [%]",
                 "IAR fixed [%]", "BR fixed [%]", "Scheduling fixed [%]"]

    lines = [
        r"\begin{table}[tbp]",
        r"\centering",
        r"\caption{Component-restricted residual regret. ``Full'' is the mean SBS "
        r"regret. Each ``fixed'' column reports the best achievable mean regret "
        r"after fixing that component to the SBS configuration while allowing "
        r"all compatible remaining components and structures to vary. Residuals "
        r"are not additive. ``---'' marks a component absent or inseparable in "
        r"the SBS's pipeline structure.}",
        r"\label{tab:appendix_stage_restriction}",
        r"\small",
        r"\setlength{\tabcolsep}{4pt}",
        r"\begin{tabular}{@{}lllr" + "r" * len(comp_cols) + r"@{}}",
        r"\toprule",
        r"Instance Set & SBS & Structure & Full"
        + "".join(rf" & {c.replace(' fixed [%]', r' fixed [\%]')}" for c in comp_cols)
        + r" \\",
        r"\midrule",
    ]
    for _, row in table.iterrows():
        cells = [
            rf"\textit{{{latex_escape(row['Instance Set'])}}}",
            latex_escape(str(row["SBS"])).replace("+", " + "),
            latex_escape(str(row["Structure"])),
            f"{row['Full regret [%]']:.2f}",
        ]
        for c in comp_cols:
            val = row.get(c)
            cells.append(f"{val:.2f}" if pd.notna(val) else NA_STR)
        lines.append(" & ".join(cells) + r" \\")
    lines += [r"\bottomrule", r"\end{tabular}", r"\end{table}", ""]
    write_text(TABLES_DIR / "appendix_stage_restriction.tex", "\n".join(lines))
    write_csv(TABLES_DIR / "appendix_stage_restriction.csv", table)
    return table


# ===========================================================================
# Appendix: runtime table
# ===========================================================================

RUNTIME_FILES = [
    "SPRP.json", "SPRP-SS.json",
    "HennWaescherUniform.json", "HennWaescherClassBased.json",
    "BahceciOencan.json", "MuterOencan.json",
    "FoodmartData.json",
    "KrisSmallDataCorrected.json", "KrisLargeData.json",
]

RUNTIME_MERGES = {
    "HennWaescher": ["HennWaescherUniform", "HennWaescherClassBased"],
    "Kris": ["KrisSmallDataCorrected", "KrisLargeData"],
}


def generate_runtimes() -> pd.DataFrame:
    """Generate the per-instance runtime table from JSON files.

    Only instance sets with available JSON files are included. The manuscript
    table is hardcoded; this output serves as a verification artifact.
    """
    import json

    rows: list[dict] = []
    for fname in RUNTIME_FILES:
        name = fname.replace(".json", "")
        path = RUNTIME_DIR / fname
        if not path.exists():
            continue
        with open(path) as f:
            data = json.load(f)
        if not data:
            continue
        build_vals = [inst.get("build_pipelines", 0) for inst in data.values()]
        run_vals = [inst.get("run_pipelines", 0) for inst in data.values()]
        rows.append({
            "instance_set": name,
            "n": len(data),
            "synthesis_mean": float(np.mean(build_vals)),
            "synthesis_std": float(np.std(build_vals)),
            "execution_mean": float(np.mean(run_vals)),
            "execution_std": float(np.std(run_vals)),
        })

    if not rows:
        print("  No runtime JSON files found; skipping runtime table.")
        return pd.DataFrame()

    table = pd.DataFrame(rows)

    # Merge split sets.
    for merged_name, parts in RUNTIME_MERGES.items():
        sub = table[table["instance_set"].isin(parts)]
        if sub.empty:
            continue
        # Combine instance counts and recompute mean/std from pooled values.
        # Since we don't have per-instance values here, approximate by weighting.
        n_total = int(sub["n"].sum())
        synth_mean = float((sub["synthesis_mean"] * sub["n"]).sum() / n_total)
        run_mean = float((sub["execution_mean"] * sub["n"]).sum() / n_total)
        table = table[~table["instance_set"].isin(parts)]
        table = pd.concat([table, pd.DataFrame([{
            "instance_set": merged_name,
            "n": n_total,
            "synthesis_mean": synth_mean,
            "synthesis_std": float(sub["synthesis_std"].mean()),
            "execution_mean": run_mean,
            "execution_std": float(sub["execution_std"].mean()),
        }])], ignore_index=True)

    # Sort by instance-set order.
    order = {n: i for i, n in enumerate(INSTANCE_ORDER)}
    table["_order"] = table["instance_set"].map(lambda x: order.get(x, 99))
    table = table.sort_values("_order").drop(columns="_order").reset_index(drop=True)

    # LaTeX.
    from data import DISPLAY_INSTANCE_NAMES as DIN
    lines = [
        r"\begin{table}[ht]",
        r"\centering",
        r"\caption{Per-instance runtimes for pipeline synthesis and portfolio execution",
        r"(mean $\pm$ standard deviation, in seconds).}",
        r"\label{tab:runtimes}",
        r"\small",
        r"\begin{tabular}{@{}lrrr@{}}",
        r"\toprule",
        r"Instance Set & $n$ & Pipeline Synthesis & Execution \\",
        r"\midrule",
    ]
    for _, row in table.iterrows():
        display = DIN.get(row["instance_set"], row["instance_set"])
        lines.append(
            rf"\textit{{{latex_escape(display)}}} "
            rf"& {int(row['n']):,} "
            rf"& ${row['synthesis_mean']:.3f} \pm {row['synthesis_std']:.3f}$ "
            rf"& ${row['execution_mean']:.3f} \pm {row['execution_std']:.3f}$" + r" \\"
        )
    lines += [r"\bottomrule", r"\end{tabular}", r"\end{table}", ""]
    write_text(TABLES_DIR / "runtimes.tex", "\n".join(lines))
    write_csv(TABLES_DIR / "runtimes.csv", table)
    return table


# ===========================================================================
# Appendix: loader runtime + cache impact
# ===========================================================================

def generate_loader_runtimes(df: pd.DataFrame) -> pd.DataFrame:
    """Per-instance-set loader timing and layout-cache impact.

    Reports mean layout/instance load times (cache misses only) and the
    layout cache hit rate, quantifying the one-time loading cost that is
    shared across pipelines versus the per-instance parsing cost.
    """
    if "layout_load_time" not in df.columns or df["layout_load_time"].isna().all():
        print("  No loader-timing data (legacy cache or missing columns);"
              " skipping loader-runtimes table.")
        return pd.DataFrame()

    rows: list[dict] = []
    for instance_set in INSTANCE_ORDER:
        subset = df[df["instance_set"] == instance_set]
        if subset.empty:
            continue
        display = DISPLAY_INSTANCE_NAMES.get(instance_set, instance_set)

        layout_times = to_numeric_nonempty(subset["layout_load_time"])
        instance_times = to_numeric_nonempty(subset["instance_load_time"])
        layout_hit = subset["layout_cache_hit"]
        instance_hit = subset["instance_cache_hit"]

        layout_miss_mask = ~layout_hit.fillna(True).astype(bool)
        instance_miss_mask = ~instance_hit.fillna(True).astype(bool)

        layout_miss_mean = float(layout_times[layout_miss_mask].mean()) if layout_miss_mask.any() else 0.0
        instance_miss_mean = float(instance_times[instance_miss_mask].mean()) if instance_miss_mask.any() else 0.0

        n_layout_hits = int(layout_hit.fillna(False).astype(bool).sum())
        n_layout_total = len(subset)
        layout_hit_rate = (100.0 * n_layout_hits / n_layout_total) if n_layout_total else 0.0

        n_instance_hits = int(instance_hit.fillna(False).astype(bool).sum())
        instance_hit_rate = (100.0 * n_instance_hits / n_layout_total) if n_layout_total else 0.0

        # Estimated time saved: cache hits * mean cache-miss time.
        layout_savings = n_layout_hits * layout_miss_mean
        instance_savings = n_instance_hits * instance_miss_mean

        rows.append({
            "Instance Set": display,
            "n_pipelines": n_layout_total,
            "layout_load_mean": layout_miss_mean,
            "instance_load_mean": instance_miss_mean,
            "layout_cache_hit_rate": layout_hit_rate,
            "instance_cache_hit_rate": instance_hit_rate,
            "layout_cache_savings": layout_savings,
            "instance_cache_savings": instance_savings,
        })

    table = pd.DataFrame(rows)
    if table.empty:
        return table

    write_csv(TABLES_DIR / "loader_runtimes.csv", table)

    lines = [
        r"\begin{table}[ht]",
        r"\centering",
        r"\caption{Loader runtimes (cache-miss means, in seconds) and cache",
        r"hit rates. ``Savings'' is the estimated wall-clock time avoided by",
        r"reusing cached domain objects across pipelines.}",
        r"\label{tab:loader_runtimes}",
        r"\small",
        r"\setlength{\tabcolsep}{4pt}",
        r"\begin{tabular}{@{}lrrrrrr@{}}",
        r"\toprule",
        r"Instance Set & $n$ & Layout [s] & Instance [s]"
        r" & Layout hit [\%] & Inst.\ hit [\%] & Savings [s] \\",
        r"\midrule",
    ]
    for _, row in table.iterrows():
        lines.append(
            rf"\textit{{{latex_escape(row['Instance Set'])}}} "
            rf"& {int(row['n_pipelines']):,} "
            rf"& {row['layout_load_mean']:.3f} "
            rf"& {row['instance_load_mean']:.3f} "
            rf"& {row['layout_cache_hit_rate']:.1f} "
            rf"& {row['instance_cache_hit_rate']:.1f} "
            rf"& {row['layout_cache_savings'] + row['instance_cache_savings']:.1f}"
            + r" \\"
        )
    lines += [r"\bottomrule", r"\end{tabular}", r"\end{table}", ""]
    write_text(TABLES_DIR / "loader_runtimes.tex", "\n".join(lines))
    return table


# ===========================================================================
# Main
# ===========================================================================

LEGACY_ONLY_NAMES = {
    "LSBatchingRR",
    "LSBatchingNNRand",
    "LSRANDNN",
    "LSRANDRR",
    "LSFiFoRR",
}


def fail_if_legacy_results(df: pd.DataFrame) -> None:
    """Fail loudly if the dataframe contains any legacy implementation results.

    Legacy-only names (e.g. LSBatchingRR, LSBatchingNNRand) have no current
    configured equivalent.  Their presence means the df was built from a run
    that used legacy modules, not the current configuration-driven path.
    """
    for col in ("batching_algo", "routing_algo", "item_assignment_algo", "scheduling_algo"):
        if col not in df.columns:
            continue
        vals = set(df[col].dropna().astype(str))
        legacy_found = vals & LEGACY_ONLY_NAMES
        if legacy_found:
            raise RuntimeError(
                f"Legacy implementation results detected in column '{col}': "
                f"{sorted(legacy_found)}. "
                f"These are not current configuration-driven results. "
                f"Rebuild df_results.pkl from current-path experiment outputs "
                f"before generating manuscript tables."
            )


def main() -> None:
    global TABLES_DIR
    parser = argparse.ArgumentParser(description="Generate all paper and appendix tables.")
    parser.add_argument("--cache", type=Path, default=CACHE_PATH)
    parser.add_argument("--tables-dir", type=Path, default=TABLES_DIR)
    parser.add_argument(
        "--allow-legacy", action="store_true",
        help="Suppress the legacy-result guard (NOT recommended for manuscript tables).",
    )
    args = parser.parse_args()

    TABLES_DIR = args.tables_dir
    TABLES_DIR.mkdir(parents=True, exist_ok=True)

    print("Loading dataframe...")
    df = pd.read_pickle(args.cache)
    print(f"  {df.shape[0]:,} rows, {df.shape[1]} columns")

    if not args.allow_legacy:
        fail_if_legacy_results(df)

    print("Preparing dataframe (unified portfolio, no RR-NF exclusion)...")
    df = prepare_df_results(df)
    df = add_structure(df)

    print("\nPipeline-structure counts:")
    print(df.groupby("instance_set")["structure"].value_counts().to_string())

    print("\n--- Pipeline space ---")
    generate_pipeline_space(df)

    print("\n--- Validation (literature comparison) ---")
    generate_validation(df)

    print("\n--- Selection (SBS regret + # Winners) ---")
    generate_selection(df)

    print("\n--- Appendix Table A: winner distribution ---")
    generate_winner_distribution(df)

    print("\n--- Appendix Table B: regret exceedance ---")
    generate_regret_exceedance(df)

    print("\n--- Appendix Table C: component-restricted residual ---")
    generate_component_restriction(df)

    print("\n--- Runtime table ---")
    generate_runtimes()

    print("\n--- Loader runtime table ---")
    generate_loader_runtimes(df)

    print("\n--- Copying appendix tables to paper_agent/tables/ ---")
    copy_to_paper_tables()

    print("\nAll tables generated in", TABLES_DIR)


if __name__ == "__main__":
    main()
