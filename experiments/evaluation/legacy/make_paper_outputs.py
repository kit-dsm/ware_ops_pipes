"""
Generate the paper tables and figures from df_results.pkl.

Everything that compares pipelines across instances is computed on PER-INSTANCE
gaps to that instance's VBS, not on mean objective values. The sets span two
orders of magnitude in instance size, so a mean over raw objectives is decided
by the largest instances: that is what made a savings pipeline the Foodmart SBS
although local search beats it on most of the set.

Every objective in the table uses the same relative gap, which is scale
invariant and therefore independent of the unit a column is stored in. The one
exception is tardiness and lateness in the rank figure: they are zero on every
deadline-feasible solution, so a relative gap has a zero denominator exactly
where the objective matters, and they are reported as absolute differences in
the unit of the cache. Each objective declares this in the Objective spec.

Outputs
    tables/pipeline_results.{tex,csv}     pipeline counts per instance set
    tables/vbs_overview_all.{tex,csv}     SBS, its gap to the VBS, per objective
    images/foodmart_gap_runtime.png       gap and runtime over instance size
    images/kris_strategy_ranks.pdf        rank consistency across objectives
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.patches as mpatches
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns


# ===========================================================================
# Naming and display
# ===========================================================================

SHORT_NAMES = {
    "Algorithm": "GIA",
    "ClosestDepotMinDistanceSeedBatching": "SEEDCDMinDist",
    "RawInput": "RawInput",
    "OrderNrFiFo": "OrdNr",
    "OrderNrFifoBatching": "OrdNr",
    "FifoBatching": "FiFo",
    "FiFo": "FiFo",
    "DueDateBatching": "DueDate",
    "ClarkAndWrightSShape": "SavingsSShape",
    "Random": "RAND",
    "RandomBatching": "RAND",
    "ExactSolving": "TSP",
    "RatliffRosenthalRouting": "RR",
    "closest_to_depot_shared_articles_SeedBatching": "SEEDCDMaxArticles",
    "ClosestDepotMaxSharedArticlesSeedBatching": "SEEDCDMaxArticles",
    "closest_to_depot_min_distance_SeedBatching": "SEEDCDMinDist",
    "RatliffRosenthalSavingsBatching": "SavingsRR",
    "NearestNeighbourhoodRouting_SavingsBatching": "SavingsNN",
    "SShapeRouting_SavingsBatching": "SavingsSShape",
    "RatliffRosenthalRouting_SavingsBatching": "SavingsRR",
    "ClarkAndWrightRR": "SavingsRR",
    "ClarkAndWrightNN": "SavingsNN",
    "RatliffRosenthalRouting_OrderNrFiFoBatching_LocalSearchBatching": "LSFiFoRR",
    "RatliffRosenthalRouting_RandomBatching_LocalSearchBatching": "LSRANDRR",
    "NearestNeighbourhoodRouting_RandomBatching_LocalSearchBatching": "LSRANDNN",
    "LSBatchingNNRand": "LSRANDNN",
    "NearestNeighbourhoodRouting_FiFoBatching_LocalSearchBatching": "LSFiFoNN",
    "NearestNeighbourhoodRouting_DueDateBatching_LocalSearchBatching": "LSDueDateNN",
    "LSBatchingNNDueDate": "LSDueDateNN",
    "LSBatchingNNFiFo": "LSFiFoNN",
    "NearestNeighbourhoodRouting_OrderNrFiFoBatching_LocalSearchBatching": "LSOrdNrNN",
    "LSBatchingNNFiFoOrderNr": "LSOrdNrNN",
    "RawPickListGeneration": "RawInput",
    "SShapeRouting": "SShape",
    "MidpointRouting": "MP",
    "Midpoint": "MP",
    "LargestGapRouting": "LG",
    "LargestGap": "LG",
    "ReturnRouting": "RET",
    "Return": "RET",
    "RatliffRosenthal": "RR",
    "NearestNeighbourhoodRouting": "NN",
    "NearestNeighbourhood": "NN",
    "ExactTSPRoutingDistance": "TSP",
    "MinMinItemAssignment": "MinMinIA",
    "NearestNeighborPickLocationSelector": "NNIA",
    "GreedyPickLocationSelector": "GIA",
    "GreedyItemAssignment": "GIA",
    "LPTScheduling": "LPT",
    "LPTScheduler": "LPT",
    "SPTScheduling": "SPT",
    "SPTScheduler": "SPT",
    "EDDScheduling": "EDD",
    "EDDScheduler": "EDD",
    "GreedyIA": "GIA",
}

ALGO_COLS = [
    "item_assignment_algo",
    "batching_algo",
    "routing_algo",
    "scheduling_algo",
]

INSTANCE_ORDER = [
    "SPRP",
    "SPRP-SS",
    "BahceciOencan",
    "HennWaescher",
    "MuterOencan",
    "FoodmartData",
    "Kris",
]

DISTANCE_SETS = [s for s in INSTANCE_ORDER if s != "Kris"]

DISPLAY_INSTANCE_NAMES = {
    "SPRP": "SPRP",
    "SPRP-SS": "SPRP-SS",
    "BahceciOencan": "BahceciOencan",
    "HennWaescher": "HennWaescher",
    "MuterOencan": "MuterOencan",
    "FoodmartData": "Foodmart",
    "Kris": "Kris",
}

PROBLEM_TYPE_MAP = {
    "SPRP": "SPRP",
    "SPRP-SS": "SPRP",
    "BahceciOencan": "OBRP",
    "HennWaescher": "OBRP",
    "MuterOencan": "OBRP",
    "FoodmartData": "OBRP",
    "Kris": "OBRSP",
}

RANK_PALETTE = [
    "#00695C", "#E65100", "#1565C0", "#4DB6AC", "#C62828",
    "#6A1B9A", "#80CBC4", "#F9A825", "#00897B", "#AD1457",
    "#2E7D32", "#5C6BC0", "#EF6C00", "#00838F", "#4E342E",
    "#78909C", "#26A69A", "#D81B60", "#558B2F", "#3949AB",
]

# A pipeline counts as reaching the VBS if its gap is within this tolerance.
GAP_TOL = 1e-9


# ===========================================================================
# Objectives
# ===========================================================================

@dataclass(frozen=True)
class Objective:
    """
    label     display name
    column    column in df_results
    maximize  True if larger is better
    gap_mode  'relative' divides the gap by the instance VBS, 'absolute' does
              not. Use absolute only where the VBS is zero on most instances,
              which is the case for tardiness and lateness. A relative gap is
              scale invariant, so it makes no assumption about the unit the
              column is stored in.
    """
    label: str
    column: str
    maximize: bool = False
    gap_mode: str = "relative"


DISTANCE = Objective("distance", "total_distance")

# Only picking time is comparable to the literature. Makespan shows whether the
# objective changes which pipeline leads, the on-time rate whether the portfolio
# separates at all on a due-date criterion. max_tardiness is not in the table:
# it is identically zero on the deadline-feasible solutions it is computed on.
KRIS_OBJECTIVES = [
    Objective("picking time", "total_time"),
    Objective("makespan", "makespan"),
    Objective("on-time rate", "on_time_rate", maximize=True),
]

# The rank figure runs over all solutions, not only the feasible ones, so the
# due-date objectives are informative there.
RANK_OBJECTIVES = [
    Objective("distance", "total_distance"),
    Objective("picking time", "total_time"),
    Objective("makespan", "makespan"),
    Objective("on-time rate", "on_time_rate", maximize=True),
    # Absolute, and in the unit of the cache: both are zero on every
    # deadline-feasible solution, so a relative gap has a zero denominator
    # exactly where the objective matters.
    Objective("max tardiness", "max_tardiness", gap_mode="absolute"),
    Objective("max lateness", "max_lateness", gap_mode="absolute"),
]

RANK_OBJECTIVE_GROUPS = {
    "Distance": ["distance"],
    "Time": ["picking time", "makespan"],
    "Due Date": ["on-time rate", "max tardiness", "max lateness"],
}


# ===========================================================================
# Frame preparation
# ===========================================================================

def to_numeric_nonempty(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def is_missing_or_empty(series: pd.Series) -> pd.Series:
    text = series.astype(str)
    return series.isna() | text.eq("") | text.eq("None") | text.eq("nan")


def nonempty_nunique(series: pd.Series) -> int:
    return int(series.replace("", np.nan).dropna().nunique())


def write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def build_strategy(row: pd.Series) -> str:
    parts = []
    for col in ALGO_COLS:
        if col not in row:
            continue
        val = row[col]
        if pd.notna(val) and str(val) != "":
            parts.append(str(val))
    return "+".join(parts)


def append_suffix_once(series: pd.Series, suffix: str) -> pd.Series:
    values = series.astype(str)
    return values.where(values.str.endswith(suffix), values + suffix)


def normalize_instance_sets(df: pd.DataFrame) -> pd.DataFrame:
    """Merge the two Henn halves and the two Kris subsets, keeping the instance
    names distinct across the merged halves."""
    df = df.copy()

    mask_cb = df["instance_set"] == "HennWaescherClassBased"
    mask_u = df["instance_set"] == "HennWaescherUniform"
    mask_small = df["instance_set"] == "KrisSmallDataCorrected"
    mask_large = df["instance_set"] == "KrisLargeData"

    if mask_cb.any():
        df.loc[mask_cb, "instance_name"] = append_suffix_once(
            df.loc[mask_cb, "instance_name"], "_cb")
    if mask_u.any():
        df.loc[mask_u, "instance_name"] = append_suffix_once(
            df.loc[mask_u, "instance_name"], "_u")
    if (mask_cb | mask_u).any():
        df.loc[mask_cb | mask_u, "instance_set"] = "HennWaescher"

    if mask_small.any():
        df.loc[mask_small, "instance_name"] = append_suffix_once(
            df.loc[mask_small, "instance_name"], "_small")
    if mask_large.any():
        df.loc[mask_large, "instance_name"] = append_suffix_once(
            df.loc[mask_large, "instance_name"], "_large")
    if (mask_small | mask_large).any():
        df.loc[mask_small | mask_large, "instance_set"] = "Kris"

    return df


def prepare_df_results(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    for col in ALGO_COLS:
        if col in df.columns:
            df[col] = df[col].replace(SHORT_NAMES)

    if "strategy" not in df.columns:
        df["strategy"] = df.apply(build_strategy, axis=1)

    df = df[df["routing_algo"]!="RatliffRosenthalNF"]
    df = df[df["instance_set"]!="KrisLargeData"]
    # Wall-clock time per pipeline execution; the cache carries the older name.
    if "total_cpu_time" not in df.columns:
        if {"routing_input_time", "total_route_time"}.issubset(df.columns):
            df["total_cpu_time"] = (
                to_numeric_nonempty(df["routing_input_time"]).fillna(0)
                + to_numeric_nonempty(df["total_route_time"]).fillna(0)
            )
        else:
            raise ValueError(
                "Could not construct total_cpu_time. Either provide it in "
                "df_results.pkl or include routing_input_time and total_route_time."
            )

    df = normalize_instance_sets(df)
    df["problem_type"] = df["instance_set"].map(PROBLEM_TYPE_MAP)
    return df


# ===========================================================================
# Per-instance gaps, shared by every table and figure below
# ===========================================================================

def add_instance_gaps(
    df: pd.DataFrame,
    obj: Objective,
    instance_col: str = "instance_name",
    context: str = "",
) -> pd.DataFrame:
    """Attach each instance's VBS value and every pipeline's gap to it."""
    work = df.copy()
    work[obj.column] = to_numeric_nonempty(work[obj.column])
    work = work.dropna(subset=[obj.column, "strategy", instance_col])

    # For a minimized objective a non-positive value cannot be divided by and
    # would also become the VBS, so those rows go. For a maximized objective a
    # zero is a legitimate worst case and must stay: only the denominator has to
    # be positive, which is handled per instance below.
    if obj.gap_mode == "relative" and not obj.maximize:
        dropped = int((work[obj.column] <= 0).sum())
        if dropped:
            print(f"[gaps] {context}/{obj.label}: {dropped} rows have a "
                  f"non-positive value and are dropped")
        work = work[work[obj.column] > 0]
    if work.empty:
        raise ValueError(f"No usable rows for {context} / {obj.column}.")

    work["vbs_value"] = work.groupby(instance_col)[obj.column].transform(
        "max" if obj.maximize else "min"
    )

    raw = (
        work["vbs_value"] - work[obj.column]
        if obj.maximize
        else work[obj.column] - work["vbs_value"]
    )

    if obj.gap_mode == "relative":
        degenerate = work.loc[work["vbs_value"] <= 0, instance_col].nunique()
        if degenerate:
            print(f"[gaps] {context}/{obj.label}: {degenerate} instances have a "
                  f"VBS of 0, so no pipeline succeeds at all there and no "
                  f"relative gap is defined; they are dropped")
        work["gap"] = raw / work["vbs_value"].where(work["vbs_value"] > 0) * 100.0
    else:
        work["gap"] = raw

    return work.dropna(subset=["gap"])


def eligible_strategies(
    work: pd.DataFrame,
    instance_col: str = "instance_name",
    context: str = "",
    reference: pd.DataFrame | None = None,
) -> list[str]:
    """
    Strategies that produced a result on every instance, so a pipeline that
    crashed or timed out on the hard instances cannot take the SBS on the easy
    ones alone.

    Coverage is counted on `reference`, which must be the frame BEFORE any
    row-level filter such as deadline feasibility. Counting it on a filtered
    frame would confuse "did not run" with "ran and was infeasible", and no
    strategy would ever be eligible.

    Returns every strategy when none covers all instances, so a row is never
    silently dropped; the printed coverage says how far off the best one is.
    """
    ref = work if reference is None else reference
    n_instances = ref[instance_col].nunique()
    coverage = ref.groupby("strategy")[instance_col].nunique()
    full = coverage[coverage == n_instances].index.tolist()

    if not full:
        best = int(coverage.max()) if len(coverage) else 0
        print(f"[gaps] {context}: no strategy ran on all {n_instances} instances "
              f"(best {best}); using all {len(coverage)} strategies, so the mean "
              f"gaps are taken over different instance subsets")
        return coverage.index.tolist()

    missing = len(coverage) - len(full)
    if missing:
        worst = coverage[coverage < n_instances].sort_values().head(3)
        print(f"[gaps] {context}: {missing} of {len(coverage)} strategies did not "
              f"run on all {n_instances} instances and are not eligible "
              f"(fewest: {dict(worst)})")
    return full


# ===========================================================================
# Table 1: pipeline counts
# ===========================================================================

def compute_pipeline_results_overview(df: pd.DataFrame) -> pd.DataFrame:
    grouped = df.groupby("instance_set", dropna=False)
    overview = grouped.agg(
        IA=("item_assignment_algo", nonempty_nunique),
        R=("routing_algo", nonempty_nunique),
        B=("batching_algo", nonempty_nunique),
        S=("scheduling_algo", nonempty_nunique),
        n_instances=("instance_name", "nunique"),
        n_pipelines=("instance_name", "count"),
    )
    overview["BR"] = 0

    # The integrated component is applicable on BahceciOencan only, where it
    # occupies the routing column of the raw frame.
    if "BahceciOencan" in overview.index:
        overview.loc["BahceciOencan", "BR"] = 1
        overview.loc["BahceciOencan", "R"] = int(overview.loc["BahceciOencan", "R"]) - 1

    return overview[["IA", "R", "B", "BR", "S", "n_instances", "n_pipelines"]]


def make_pipeline_count_table_latex(overview: pd.DataFrame) -> str:
    pro = overview.reindex(INSTANCE_ORDER).copy()

    rows = []
    for idx, row in pro.iterrows():
        name = DISPLAY_INSTANCE_NAMES.get(idx, idx)
        vals = " & ".join(str(int(v)) if pd.notna(v) else "--" for v in row)
        rows.append(rf"\textit{{{name}}} & {vals} \\")

    return "\n".join([
        r"\begin{table}[t]",
        r"\centering",
        r"\caption{Number of resulting pipelines per instance set.}",
        r"\label{tab:pipeline_results}",
        r"\begin{tabular}{@{}lrrrrrrr@{}}",
        r"\toprule",
        r"  & \multicolumn{5}{c}{\# Algorithms} & \# Instances & \# Pipelines\\",
        r"\cmidrule(lr){2-6}",
        r"Instance Set & IA & R & B & BR & S &  &  \\",
        r"\midrule",
        *rows,
        r"\midrule",
        rf"$\sum$ &  &  &  &  &  & {int(pro['n_instances'].sum()):,} "
        rf"& {int(pro['n_pipelines'].sum()):,}\\",
        r"\bottomrule",
        r"\end{tabular}",
        r"\end{table}",
    ]) + "\n"


# ===========================================================================
# Table 2: SBS against VBS
# ===========================================================================

def compute_sbs_vbs_row(
    df: pd.DataFrame,
    obj: Objective,
    instance_set: str,
    instance_col: str = "instance_name",
    reference_df: pd.DataFrame | None = None,
) -> dict:
    """
    reference_df is the frame before any row-level filter, used only to decide
    which strategies ran everywhere. Pass it whenever df has been filtered by
    something other than the instance set.
    """
    display = DISPLAY_INSTANCE_NAMES.get(instance_set, instance_set)
    work = add_instance_gaps(df, obj, instance_col, context=display)

    reference = None
    if reference_df is not None:
        reference = reference_df[to_numeric_nonempty(reference_df[obj.column]).notna()]
        if reference.empty:
            reference = None

    candidates = eligible_strategies(
        work, instance_col, f"{display}/{obj.label}", reference=reference
    )
    if not candidates:
        raise ValueError(f"No strategy has results on {display}.")

    mean_gap = work[work["strategy"].isin(candidates)].groupby("strategy")["gap"].mean()
    sbs = mean_gap.idxmin()

    n_instances = work[instance_col].nunique()
    sbs_at_vbs = (
        work[(work["strategy"] == sbs) & (work["gap"].abs() <= GAP_TOL)][instance_col]
        .nunique()
    )

    return {
        "Problem": PROBLEM_TYPE_MAP.get(instance_set, ""),
        "Instance Set": display,
        "Objective": obj.label,
        "SBS": sbs,
        "Mean Gap": float(mean_gap.min()),
        # Spread of the mean gap over the eligible pipelines. If the worst is
        # close to the SBS, the objective does not separate the portfolio.
        "Median Gap": float(mean_gap.median()),
        "Worst Gap": float(mean_gap.max()),
        "# Inst.": int(n_instances),
        "# Cand.": len(candidates),
        # Raw means, for scale only. They do NOT reproduce the gap column, which
        # is a mean over per-instance ratios rather than a ratio of means.
        "SBS Mean": float(work.loc[work["strategy"] == sbs, obj.column].mean()),
        "VBS Mean": float(work.groupby(instance_col)["vbs_value"].first().mean()),
        "SBS at VBS [%]": sbs_at_vbs / n_instances * 100,
    }


# Columns worth reading in the console. The CSV keeps everything.
PRINT_COLS = [
    "Instance Set", "Objective", "SBS", "Mean Gap", "Median Gap", "Worst Gap",
    "SBS at VBS [%]", "# Inst.", "# Cand.",
]


def kris_frame(
    df: pd.DataFrame,
    feasible_only: bool = False,
    common_instances: bool = True,
) -> pd.DataFrame:
    """
    Kris rows are computed on all solutions by default, and on the instances for
    which every objective has values.

    feasible_only drops individual runs that miss a deadline, which makes each
    pipeline's mean gap a mean over the instances where that pipeline happens to
    be feasible. A pipeline feasible on a third of the set is then compared on
    its easiest third. Keep the deadline analysis in the feasibility table and
    leave this one over all solutions unless you have a reason not to.

    common_instances intersects the instances across the objectives. The large
    runs predate the total_time aggregation, so without it the picking-time row
    covers fewer instances than the other two and the rows are not comparable.
    """
    kris = df[df["instance_set"] == "Kris"].copy()
    if kris.empty:
        return kris
    if "scheduling_algo" in kris.columns:
        kris = kris[~is_missing_or_empty(kris["scheduling_algo"])]
    if feasible_only and "max_lateness" in kris.columns:
        kris = kris[to_numeric_nonempty(kris["max_lateness"]) <= GAP_TOL]

    if common_instances:
        idx = None
        for obj in KRIS_OBJECTIVES:
            if obj.column not in kris.columns:
                continue
            have = kris.loc[
                to_numeric_nonempty(kris[obj.column]).notna(), "instance_name"
            ].unique()
            idx = pd.Index(have) if idx is None else idx.intersection(have)
        if idx is not None:
            dropped = kris["instance_name"].nunique() - len(idx)
            if dropped:
                print(f"[vbs] Kris: {dropped} instances lack values for at least "
                      f"one objective and are excluded so the rows share instances")
            kris = kris[kris["instance_name"].isin(idx)]
    return kris


def build_vbs_overview_table(
    df: pd.DataFrame,
    kris_feasible_only: bool = False,
    kris_common_instances: bool = True,
) -> pd.DataFrame:
    rows: list[dict] = []

    for instance_set in DISTANCE_SETS:
        subset = df[df["instance_set"] == instance_set].copy()
        if subset.empty:
            print(f"[vbs] no rows for {instance_set}, skipped")
            continue
        # The distance sets have no scheduling stage.
        if "scheduling_algo" in subset.columns:
            subset = subset[is_missing_or_empty(subset["scheduling_algo"])]
        try:
            rows.append(compute_sbs_vbs_row(subset, DISTANCE, instance_set))
        except ValueError as exc:
            print(f"[vbs] {instance_set}: {exc}")

    kris = kris_frame(df, kris_feasible_only, kris_common_instances)
    # Coverage reference: the same instances, but before the feasibility filter,
    # so an infeasible run still counts as a run.
    kris_ref = kris_frame(df, feasible_only=False, common_instances=kris_common_instances)
    if not kris.empty:
        for obj in KRIS_OBJECTIVES:
            if obj.column not in kris.columns or kris[obj.column].notna().sum() == 0:
                print(f"[vbs] Kris/{obj.label}: column {obj.column} missing, skipped")
                continue
            try:
                rows.append(
                    compute_sbs_vbs_row(kris, obj, "Kris", reference_df=kris_ref)
                )
            except ValueError as exc:
                print(f"[vbs] Kris/{obj.label}: {exc}")

    out = pd.DataFrame(rows)
    if out.empty:
        return out

    order = {DISPLAY_INSTANCE_NAMES[s]: i for i, s in enumerate(INSTANCE_ORDER)}
    out["_o"] = out["Instance Set"].map(order)
    return (
        out.sort_values("_o", kind="mergesort")
        .drop(columns="_o")
        .reset_index(drop=True)
    )


def make_vbs_overview_latex(
    df_vbs: pd.DataFrame,
    include_raw_means: bool = False,
) -> str:
    """
    The gap column holds percent for the relative objectives and percentage
    points for the on-time rate, which the objective label carries.
    """
    body = []
    previous_set = None
    for _, row in df_vbs.iterrows():
        first = (rf"\textit{{{row['Instance Set']}}}"
                 if row["Instance Set"] != previous_set else "")
        previous_set = row["Instance Set"]
        cells = [
            first,
            row["Objective"],
            str(row["SBS"]).replace("+", " + "),
            f"{row['Mean Gap']:.2f}",
            f"{int(row['# Inst.']):,}",
        ]
        if include_raw_means:
            cells += [f"{row['SBS Mean']:.1f}", f"{row['VBS Mean']:.1f}"]
        body.append(" & ".join(cells) + r"\\")

    header = r"Instance Set & Objective & SBS & Gap [\%] & \# Inst."
    fmt = "lllrr"
    if include_raw_means:
        header += " & SBS Mean & VBS Mean"
        fmt += "rr"

    return "\n".join([
        r"\begin{table}[t]",
        r"\centering",
        r"\caption{Gap of the SBS to the VBS, as the mean over per-instance "
        r"gaps.}",
        r"\label{tab:vbs_overview_all}",
        r"\small",
        rf"\begin{{tabular}}{{@{{}}{fmt}@{{}}}}",
        r"\toprule",
        header + r"\\",
        r"\midrule",
        *body,
        r"\bottomrule",
        r"\end{tabular}",
        r"\end{table}",
    ]) + "\n"


# ===========================================================================
# Figure 1: Foodmart gap to the VBS and runtime over instance size
# ===========================================================================

def prepare_foodmart_plot_data(df: pd.DataFrame, top_k: int = 7) -> pd.DataFrame:
    fm = df[df["instance_set"] == "FoodmartData"].copy()
    if fm.empty:
        raise ValueError("No FoodmartData rows found in df_results.pkl.")

    fm["instance_key"] = (
        fm["instance_name"].astype(str).str.replace(r"^instances_|_MAL$", "", regex=True)
    )
    fm["n_orders"] = to_numeric_nonempty(fm["instance_key"].str.extract(r"ord(\d+)")[0])
    fm["total_cpu_time"] = to_numeric_nonempty(fm["total_cpu_time"])
    fm = fm.dropna(subset=["n_orders", "total_cpu_time"])

    # Gaps are per instance, then averaged within a size, so a large instance
    # inside a size bucket cannot dominate the curve either.
    work = add_instance_gaps(fm, DISTANCE, context="Foodmart")
    perf = (
        work.groupby(["n_orders", "strategy"], as_index=False)
        .agg(gap_pct=("gap", "mean"), total_cpu_time=("total_cpu_time", "mean"))
    )

    order = (
        perf.groupby("strategy", as_index=False)["gap_pct"].mean()
        .sort_values(["gap_pct", "strategy"])
        .head(top_k)["strategy"].tolist()
    )
    perf = perf[perf["strategy"].isin(order)].copy()
    perf["strategy"] = pd.Categorical(perf["strategy"], categories=order, ordered=True)
    return perf.sort_values(["strategy", "n_orders"])


def plot_foodmart_gap_runtime(perf: pd.DataFrame, output_path: Path) -> None:
    sns.set_style("whitegrid", {"axes.grid": False})
    sns.set_context("talk")

    sizes = sorted(perf["n_orders"].dropna().unique())
    strategies = list(perf["strategy"].cat.categories)
    palette = RANK_PALETTE[: len(strategies)]

    fig, axes = plt.subplots(2, 1, figsize=(14, 8), sharex=True)
    fig.patch.set_facecolor("white")
    for ax in axes:
        ax.grid(axis="y", alpha=0.3)

    sns.lineplot(x="n_orders", y="gap_pct", hue="strategy", data=perf,
                 ax=axes[0], sort=False, palette=palette, marker="o")
    axes[0].set_ylabel("Gap to VBS (%)")
    axes[0].set_xlabel("")

    sns.lineplot(x="n_orders", y="total_cpu_time", hue="strategy", data=perf,
                 ax=axes[1], sort=False, palette=palette, marker="o")
    axes[1].set_ylabel("Mean runtime (s)")
    axes[1].set_xlabel("n orders")

    for ax in axes:
        ax.set_xticks(sizes)
        ax.set_xticklabels([str(int(x)) for x in sizes])
    axes[1].tick_params(axis="x", rotation=45)

    axes[1].legend(title="Pipeline")
    if axes[0].get_legend() is not None:
        axes[0].get_legend().remove()

    plt.tight_layout()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(output_path, dpi=300, bbox_inches="tight", pad_inches=0)
    plt.close(fig)


# ===========================================================================
# Figure 2: Kris rank consistency across objectives
# ===========================================================================

def compute_pipeline_ranks(
    df: pd.DataFrame,
    objectives: list[Objective],
    context: str = "Kris",
) -> pd.DataFrame:
    """Rank pipelines per objective by their mean per-instance gap."""
    records: list[dict] = []

    for obj in objectives:
        if obj.column not in df.columns or df[obj.column].notna().sum() == 0:
            print(f"[ranks] {context}/{obj.label}: column {obj.column} missing, skipped")
            continue
        try:
            work = add_instance_gaps(df, obj, context=context)
        except ValueError as exc:
            print(f"[ranks] {context}/{obj.label}: {exc}")
            continue

        candidates = eligible_strategies(work, context=f"{context}/{obj.label}")
        work = work[work["strategy"].isin(candidates)]
        if work.empty:
            continue

        mean_gap = (
            work.groupby("strategy", as_index=False)["gap"].mean()
            .sort_values(["gap", "strategy"]).reset_index(drop=True)
        )
        mean_gap["rank"] = range(1, len(mean_gap) + 1)
        records += [
            {"pipeline": r["strategy"], "objective": obj.label, "rank": r["rank"]}
            for _, r in mean_gap.iterrows()
        ]

    return pd.DataFrame(records)


def plot_rank_consistency(
    df_ranks: pd.DataFrame,
    output_path: Path,
    objective_groups: dict[str, list[str]] | None = None,
    top_k: int = 10,
    figsize: tuple[int, int] = (12, 6),
) -> None:
    sns.set_style("whitegrid", {"axes.grid": False})
    sns.set_context("talk", rc={"axes.labelsize": 14, "xtick.labelsize": 14,
                                "ytick.labelsize": 14})

    objectives = list(dict.fromkeys(df_ranks["objective"]))
    df_plot = df_ranks[df_ranks["rank"] <= top_k].copy()
    pipelines = sorted(df_plot["pipeline"].unique())
    color_map = {p: RANK_PALETTE[i % len(RANK_PALETTE)] for i, p in enumerate(pipelines)}

    fig, ax = plt.subplots(figsize=figsize)

    if objective_groups:
        group_colors = ["#e8d0d0", "#d0e8d0", "#d0d0e8"]
        for gi, (group_name, group_objs) in enumerate(objective_groups.items()):
            idx = [objectives.index(o) for o in group_objs if o in objectives]
            if not idx:
                continue
            x0, x1 = min(idx) - 0.45, max(idx) + 0.45
            ax.axvspan(x0, x1, alpha=0.25, color=group_colors[gi % len(group_colors)],
                       zorder=0)
            ax.text((x0 + x1) / 2, 0.3, group_name, ha="center", va="bottom",
                    fontweight="bold", color="#444")

    for _, row in df_plot.iterrows():
        ax.scatter(objectives.index(row["objective"]), row["rank"],
                   color=color_map[row["pipeline"]], s=100, zorder=3,
                   edgecolors="black", linewidths=0.5)

    for pipeline in pipelines:
        df_p = df_plot[df_plot["pipeline"] == pipeline]
        if len(df_p) < 2:
            continue
        xs = np.array([objectives.index(o) for o in df_p["objective"]])
        ys = df_p["rank"].to_numpy()
        order = np.argsort(xs)
        ax.plot(xs[order], ys[order], color=color_map[pipeline], alpha=0.35,
                linewidth=1.2, zorder=2, linestyle="--")

    ax.set_xticks(range(len(objectives)))
    ax.set_xticklabels(objectives, rotation=35, ha="right")
    ax.set_ylabel("Rank")
    ax.set_yticks(range(1, top_k + 1))
    ax.set_ylim(top_k + 0.5, 0.5)
    ax.set_xlim(-0.6, len(objectives) - 0.4)
    ax.grid(axis="y", alpha=0.2, linestyle="--")

    handles = [mpatches.Patch(color=color_map[p], label=p) for p in pipelines]
    ax.legend(handles=handles, bbox_to_anchor=(1.02, 1), loc="upper left",
              title="Pipeline", fontsize=10)

    plt.tight_layout()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(output_path, bbox_inches="tight", dpi=300)
    plt.close(fig)


# ===========================================================================
# Main
# ===========================================================================

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate paper tables and figures from df_results.pkl."
    )
    parser.add_argument("--df-results", type=Path, default=Path("./df_results.pkl"))
    parser.add_argument("--images-dir", type=Path, default=Path("./images"))
    parser.add_argument("--tables-dir", type=Path, default=Path("./tables"))
    parser.add_argument("--top-k-foodmart", type=int, default=7)
    parser.add_argument("--top-k-kris", type=int, default=10)
    parser.add_argument("--raw-means", action="store_true",
                        help="also print SBS Mean and VBS Mean in the VBS table")
    parser.add_argument("--kris-feasible-only", action="store_true",
                        help="restrict the Kris rows to runs that meet every "
                             "deadline; biases each pipeline's gap towards the "
                             "instances where it is feasible")
    parser.add_argument("--kris-all-instances", action="store_true",
                        help="do not restrict the Kris rows to instances that "
                             "have values for every objective")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if not args.df_results.exists():
        raise FileNotFoundError(f"df_results file not found: {args.df_results}")

    df = prepare_df_results(pd.read_pickle(args.df_results))
    args.images_dir.mkdir(parents=True, exist_ok=True)
    args.tables_dir.mkdir(parents=True, exist_ok=True)

    # --- Table: pipeline counts -------------------------------------------
    overview = compute_pipeline_results_overview(df)
    overview.to_csv(args.tables_dir / "pipeline_results.csv")
    write_text(args.tables_dir / "pipeline_results.tex",
               make_pipeline_count_table_latex(overview))

    # --- Table: SBS against VBS -------------------------------------------
    df_vbs = build_vbs_overview_table(
        df,
        kris_feasible_only=args.kris_feasible_only,
        kris_common_instances=not args.kris_all_instances,
    )
    if df_vbs.empty:
        print("[vbs] no rows produced")
    else:
        print("\n" + df_vbs[PRINT_COLS].round(2).to_string(index=False) + "\n")
        df_vbs.round(2).to_csv(args.tables_dir / "vbs_overview_all.csv", index=False)
        write_text(args.tables_dir / "vbs_overview_all.tex",
                   make_vbs_overview_latex(df_vbs, include_raw_means=args.raw_means))

    # --- Figure: Foodmart gap and runtime ----------------------------------
    try:
        perf = prepare_foodmart_plot_data(df, top_k=args.top_k_foodmart)
        plot_foodmart_gap_runtime(perf, args.images_dir / "foodmart_gap_runtime.png")
    except ValueError as exc:
        print(f"[foodmart] skipped: {exc}")

    # --- Figure: Kris ranks -------------------------------------------------
    # Over all solutions, not only the feasible ones, so the due-date
    # objectives say something.
    kris = kris_frame(df, feasible_only=False, common_instances=False)
    if kris.empty:
        print("[ranks] no Kris rows")
    else:
        df_ranks = compute_pipeline_ranks(kris, RANK_OBJECTIVES)
        if not df_ranks.empty:
            plot_rank_consistency(
                df_ranks,
                output_path=args.images_dir / "kris_strategy_ranks.pdf",
                objective_groups=RANK_OBJECTIVE_GROUPS,
                top_k=args.top_k_kris,
            )

    print("Generated outputs in "
          f"{args.tables_dir} and {args.images_dir}")


if __name__ == "__main__":
    main()