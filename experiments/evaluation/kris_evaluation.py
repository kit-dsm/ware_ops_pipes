from pathlib import Path
import re

import numpy as np
import pandas as pd
from matplotlib import pyplot as plt
import seaborn as sns

CACHE_PATH = Path("./df_results.pkl")

SPRP_RESULTS_PATH = Path("../../data/results/results_SPRP.csv")
SPRP_SS_RESULTS_PATH = Path("../../data/results/results_SPRP-SS.csv")
BAHCECI_OENCAN_RESULTS_PATH = Path("../../data/results/results_BahceciOencan.csv")
HENN_WAESCHER_RESULTS_PATH = Path("../../data/results/results_HennWaescher.csv")
MUTER_OENCAN_RESULTS_PATH = Path("../../data/results/results_Muter.csv")
FOODMART_RESULTS_PATH = Path("../../data/results/results_Foodmart.csv")

KRIS_SMALL_SOLUTIONS_PATH = Path("../../data/results/allSolutions/solutionssmall/")
KRIS_LARGE_SOLUTIONS_PATH = Path("../../data/results/allSolutions/solutionslarge/")

# Tolerance for treating a gap as zero / for solver agreement.
GAP_TOL = 1e-6


# =============================================================================
# Helpers
# =============================================================================

def read_result_csv(path: Path, skiprows=None) -> pd.DataFrame:
    return pd.read_csv(path, sep=";", decimal=",", thousands=".", skiprows=skiprows)


def gap_min(df: pd.DataFrame, obj_col: str, ref_col: str = "reference_value") -> pd.Series:
    return ((df[obj_col] - df[ref_col]) / df[ref_col]) * 100


def reference_type_from_bounds(
    df: pd.DataFrame, time_col: str = "time [s]", time_limit: float = 3600.0
) -> pd.Series:
    """
    OBRP sets with an explicit opt-flag AND matching bounds -- but only trust
    it when the solver converged *before* the wall-clock limit. Rows that sit
    at the time limit report opt?=true / UB==LB optimistically (the incumbent
    is echoed into LB); they are feasible upper bounds, not proven optima.
    Downgrading them keeps # Proven Opt. honest: the incumbent counts as a
    reported feasible solution, not a proven optimum.
    """
    hit_limit = df[time_col].astype(float) >= time_limit
    proven = (
        (df["UB"].round(6) == df["LB"].round(6))
        & (df["opt?"] == True)
        & (~hit_limit)
    )
    return np.where(proven, "optimum", "reported feasible solution")


def reference_type_from_lb_ub(df: pd.DataFrame) -> pd.Series:
    """Foodmart: no opt-flag, so bound equality is the only optimality signal."""
    return np.where(
        df["UB"].round(6) == df["LB"].round(6),
        "optimum",
        "reported feasible solution",
    )


def reference_from_solvers(df: pd.DataFrame, cost_cols: list[str]) -> pd.DataFrame:
    """
    SPRP / SPRP-SS have no opt-flag and no LB, but report several independent
    *exact* solver costs. The best-known value is the min across whatever
    solvers are present; optimality is certified by agreement between them
    (independent exact formulations landing on the same value). Disagreement
    means at least one solver did not prove optimality -> reported feasible.
    """
    present = [c for c in cost_cols if c in df.columns]
    if not present:
        raise KeyError(f"None of {cost_cols} found in results columns {list(df.columns)}")

    costs = df[present].astype(float)
    ref_value = costs.min(axis=1)
    agree = costs.round(6).nunique(axis=1) == 1  # all present solvers equal

    df = df.copy()
    df["reference_value"] = ref_value
    df["reference_type"] = np.where(agree, "optimum", "reported feasible solution")
    return df


def coverage_report(df_set: pd.DataFrame, set_name: str) -> None:
    """
    Report how many solved instances have a literature reference. Informational
    by design (several sets legitimately have more instances than references);
    hard-fails only on a total miss, which means the filename reconstruction /
    merge key is broken.
    """
    n_inst = df_set["instance_name"].nunique()
    n_ref = df_set.loc[df_set["reference_value"].notna(), "instance_name"].nunique()
    print(
        f"[coverage] {set_name:<14} solved={n_inst:>6} | "
        f"with reference={n_ref:>6} | without={n_inst - n_ref:>6}"
    )
    if n_ref == 0:
        raise ValueError(
            f"[{set_name}] no references matched after merge — "
            f"check the filename reconstruction / merge keys against the df."
        )


def create_row_vbs(
    df_vbs: pd.DataFrame,
    objective: str = "distance",
    ref_col: str = "reference_value",
) -> pd.DataFrame:
    """
    Build one validation row. The gap is the per-instance VBS (best synthesized
    pipeline per instance) against the source paper's reported reference value,
    averaged over the instances that have one. Gap to Opt restricts that to the
    subset the source proves optimal. All statistics are on the same referenced
    subset, so # with Reference is the denominator throughout.
    """
    set_label = df_vbs["instance_set"].iloc[0]
    ref = df_vbs[df_vbs[ref_col].notna()].copy()
    is_opt = ref["reference_type"] == "optimum"

    # Sanity: a proven optimum cannot be beaten. A negative gap on the optimum
    # subset means a reference/objective/rounding mismatch — surface it loudly.
    bad = ref.loc[is_opt & (ref["gap_[%]"] < -GAP_TOL)]
    if len(bad):
        names = bad["instance_name"].tolist()[:10]
        print(
            f"[WARN] {set_label} / {objective}: "
            f"{len(bad)} proven-optimum instances have VBS better than the "
            f"optimum (impossible) — check reference/objective/rounding:\n"
            f"       {names}{' ...' if len(bad) > 10 else ''}"
        )

    return pd.DataFrame([{
        "instance_set": set_label,
        "objective": objective,
        "n_instances": EVALUATED_COUNTS.get(set_label, df_vbs["instance_name"].nunique()),
        "n_referenced": ref["instance_name"].nunique(),
        "n_proven_opt": int(is_opt.sum()),
        "gap_to_bks_[%]": ref["gap_[%]"].mean(),
        "gap_to_opt_[%]": ref.loc[is_opt, "gap_[%]"].mean() if is_opt.any() else np.nan,
    }])


def parse_solution_file(filepath):
    text = Path(filepath).read_text()

    batches = []
    for m in re.finditer(
        r"PickerID\t(\d+)\tBatchID\t(\d+)\tPreviousBatch\t(\d+)\tNoOders\t(\d+)\tNoOderLines\t(\d+)\tBatchDistance\t(\d+)\tBatchComplTime\t(\d+)",
        text,
    ):
        batches.append({
            "picker_id": int(m[1]),
            "batch_id": int(m[2]),
            "n_orders": int(m[4]),
            "n_lines": int(m[5]),
            "distance": int(m[6]),
            "completion_time": int(m[7]),
        })

    orders = []
    for m in re.finditer(
        r"OrderID\t(\d+)\tNoOrderLines\t(\d+)\tNextOrderID\t(\d+)\tPickerID\t(\d+)\tBatchID\t(\d+)\tDueTime\t(\d+)\tCompletionTime\t(\d+)",
        text,
    ):
        orders.append({
            "order_id": int(m[1]),
            "due_time": int(m[6]),
            "completion_time": int(m[7]),
        })

    total_distance = sum(b["distance"] for b in batches)
    makespan = max(b["completion_time"] for b in batches)
    tardiness = sum(max(0, o["completion_time"] - o["due_time"]) for o in orders)
    max_tardiness = max(
        (max(0, o["completion_time"] - o["due_time"]) for o in orders),
        default=0,
    )
    n_tardy = sum(1 for o in orders if o["completion_time"] > o["due_time"])
    n_on_time = sum(1 for o in orders if o["completion_time"] <= o["due_time"])
    on_time_rate = n_on_time / len(orders) * 100

    return {
        "best_total_distance": total_distance,
        "best_makespan": makespan,
        "best_tardiness": tardiness,
        "best_max_tardiness": max_tardiness,
        "best_on_time_rate": on_time_rate,
        "best_n_tardy": n_tardy,
        "best_n_batches": len(batches),
        "n_orders": len(orders),
        "n_pickers": len(set(b["picker_id"] for b in batches)),
    }


def parse_solution_dir(directory, glob="*.txt"):
    rows = []
    for fp in sorted(Path(directory).glob(glob)):
        split_name = fp.stem.split("_")
        row = parse_solution_file(fp)
        row["instance_name"] = f"instances_{split_name[2]}_{split_name[3]}"
        rows.append(row)
    return pd.DataFrame(rows)


# =============================================================================
# Load pipeline results from notebook cache
# =============================================================================

df = pd.read_pickle(CACHE_PATH).copy()

# Same Henn preprocessing as notebook cells 9-11.
df.loc[df["instance_set"] == "HennWaescherClassBased", "instance_name"] = (
    df.loc[df["instance_set"] == "HennWaescherClassBased", "instance_name"] + "_cb"
)
df.loc[df["instance_set"] == "HennWaescherUniform", "instance_name"] = (
    df.loc[df["instance_set"] == "HennWaescherUniform", "instance_name"] + "_u"
)
df.loc[df["instance_set"] == "HennWaescherUniform", "instance_set"] = "HennWaescher"
df.loc[df["instance_set"] == "HennWaescherClassBased", "instance_set"] = "HennWaescher"

df.loc[df["instance_set"] == "KrisSmallDataCorrected", "instance_name"] = (
    df.loc[df["instance_set"] == "KrisSmallDataCorrected", "instance_name"] + "_small"
)
df.loc[df["instance_set"] == "KrisLargeData", "instance_name"] = (
    df.loc[df["instance_set"] == "KrisLargeData", "instance_name"] + "_large"
)
df.loc[df["instance_set"] == "KrisSmallDataCorrected", "instance_set"] = "Kris"
df.loc[df["instance_set"] == "KrisLargeData", "instance_set"] = "Kris"

# total_distance defaults to 0 in create_summary_dataframe when a pipeline has
# no tours_summary (routing did not run / failed for that pipeline). A 0 then
# sorts as the minimum and wins the distance VBS, showing up as a spurious
# -100% gap / "new best". Drop these degenerate rows before any selection.
df = df[df["total_distance"].astype(float) > 0].copy()

# Full count of instances CASOP evaluated per set, captured before any
# reference merge/dropna. This is the coverage column (# Instances) and is the
# denominator that shows the framework runs the complete benchmark while the
# literature reports on a subset (# Referenced). Keys match the instance_set
# label used in the VBS frames.
EVALUATED_COUNTS = {
    "SPRP": df[df["instance_set"] == "SPRP"]["instance_name"].nunique(),
    "SPRP-SS": df[df["instance_set"] == "SPRP-SS"]["instance_name"].nunique(),
    "BahceciOencan": df[df["instance_set"] == "BahceciOencan"]["instance_name"].nunique(),
    "HennWaescher": df[df["instance_set"] == "HennWaescher"]["instance_name"].nunique(),
    "MuterOencan": df[df["instance_set"] == "MuterOencan"]["instance_name"].nunique(),
    "Foodmart": df[df["instance_set"] == "FoodmartData"]["instance_name"].nunique(),
    "Kris": df[df["instance_set"] == "Kris"]["instance_name"].nunique(),
}


# =============================================================================
# SPRP  (exact costs from GS MIP / Netw MIP / DP -> min reference, agreement = optimum)
# =============================================================================

instance_set_sprp = "SPRP"
results_sprp = read_result_csv(SPRP_RESULTS_PATH)

# NOTE: filename reconstruction left exactly as-is — it is tuned to the df's
# instance naming from the preparation steps. `unit_F1` is hardcoded even
# though `unit demand`/`alpha` columns exist; the coverage report below will
# flag any instance this fails to match rather than silently dropping it.
results_sprp["filename"] = results_sprp.apply(
    lambda row: f"unit_F1_m{row['num aisles']}_C{row['num cells']}_a{row['num articles']}_{row['random seed']}",
    axis=1,
)
results_sprp = reference_from_solvers(
    results_sprp, ["GS MIP cost", "Netw MIP cost", "DP cost"]
)
results_sprp["reference_runtime"] = results_sprp["GS MIP time cplex [ms]"] / 1000

df_sprp = df[df["instance_set"] == instance_set_sprp].copy()
df_sprp = df_sprp.merge(
    right=results_sprp[["filename", "reference_value", "reference_runtime", "reference_type"]],
    how="left",
    left_on="instance_name",
    right_on="filename",
)
coverage_report(df_sprp, "SPRP")
df_sprp["gap_[%]"] = gap_min(df_sprp, "total_distance")

df_sprp_vbs = (
    df_sprp
    .sort_values(["total_distance", "total_cpu_time"])
    .groupby("instance_name")
    .first()
    .reset_index()
    [["instance_name", "strategy", "total_distance", "total_cpu_time",
      "reference_value", "reference_runtime", "reference_type", "gap_[%]"]]
)
df_sprp_vbs["instance_set"] = "SPRP"


# =============================================================================
# SPRP-SS  (GS MIP / Netw MIP only — no DP backstop)
# =============================================================================

instance_set_sprp_ss = "SPRP-SS"
results_sprp_ss = read_result_csv(SPRP_SS_RESULTS_PATH)

results_sprp_ss["demand_helper"] = results_sprp_ss.apply(
    lambda row: "unit" if row["unit demand"] else "varying",
    axis=1,
)
results_sprp_ss["filename"] = results_sprp_ss.apply(
    lambda row: f"{row['demand_helper']}_F{row['alpha']}_m{row['num aisles']}_C{row['num cells']}_a{row['num articles']}_{row['random seed']}",
    axis=1,
)
results_sprp_ss = reference_from_solvers(
    results_sprp_ss, ["GS MIP cost", "Netw MIP cost"]
)
results_sprp_ss["reference_runtime"] = results_sprp_ss["GS MIP time cplex [ms]"] / 1000

df_sprp_ss = df[df["instance_set"] == instance_set_sprp_ss].copy()
df_sprp_ss = df_sprp_ss.merge(
    right=results_sprp_ss[["filename", "reference_value", "reference_runtime", "reference_type"]],
    how="left",
    left_on="instance_name",
    right_on="filename",
)
coverage_report(df_sprp_ss, "SPRP-SS")
df_sprp_ss["gap_[%]"] = gap_min(df_sprp_ss, "total_distance")

df_sprp_ss_vbs = (
    df_sprp_ss
    .sort_values(["total_distance", "total_cpu_time"])
    .groupby("instance_name")
    .first()
    .reset_index()
    [["instance_name", "strategy", "total_distance", "total_cpu_time",
      "reference_value", "reference_runtime", "reference_type", "gap_[%]"]]
)
df_sprp_ss_vbs["instance_set"] = "SPRP-SS"


# =============================================================================
# Foodmart  (LB/UB, no opt-flag -> bound equality is the optimality signal)
# =============================================================================

instance_set_fm = "FoodmartData"
results_fm = read_result_csv(FOODMART_RESULTS_PATH)

results_fm["reference_value"] = results_fm["UB"]
results_fm["reference_runtime"] = results_fm["CPU Total (s)"]
results_fm["reference_type"] = reference_type_from_lb_ub(results_fm)

df_foodmart = df[df["instance_set"] == instance_set_fm].copy()
df_foodmart["instance_name"] = df_foodmart["instance_name"].str.replace(
    r"^instances_|_MAL$",
    "",
    regex=True,
)
df_foodmart = df_foodmart.merge(
    right=results_fm[["Name", "reference_value", "reference_runtime", "reference_type"]],
    how="left",  # keep ALL solved instances; only 42 of them have a reference
    left_on="instance_name",
    right_on="Name",
)
coverage_report(df_foodmart, "Foodmart")
df_foodmart["gap_[%]"] = gap_min(df_foodmart, "total_distance")

df_foodmart_vbs = df_foodmart.loc[
    df_foodmart.groupby("instance_name")["total_distance"].idxmin(),
    ["instance_name", "strategy", "total_distance", "total_cpu_time",
     "gap_[%]", "reference_value", "reference_runtime", "reference_type"],
].copy()
df_foodmart_vbs["instance_set"] = "Foodmart"


# =============================================================================
# Henn/Waescher  (opt-flag + bounds)
# =============================================================================

instance_set_hw = "HennWaescher"
results_hw = read_result_csv(HENN_WAESCHER_RESULTS_PATH)

results_hw["storage_policy"] = (
    results_hw["filename"].str.split("\\").str[0].str.split("_").str[1]
)
results_hw["filename"] = (
    results_hw["filename"].str.split("\\").str[-1].str.replace(".txt", "", regex=False)
)

results_hw.loc[results_hw["storage_policy"] == "uniform", "filename"] = (
    results_hw.loc[results_hw["storage_policy"] == "uniform", "filename"] + "_u"
)
results_hw.loc[results_hw["storage_policy"] == "class-based", "filename"] = (
    results_hw.loc[results_hw["storage_policy"] == "class-based", "filename"] + "_cb"
)

# Keep the exact-routing ("optimal" policy) rows as the reference.
results_hw = results_hw[results_hw["policy"] == "optimal"].copy()
results_hw["reference_value"] = results_hw["UB"]
results_hw["reference_runtime"] = results_hw["time [s]"]
results_hw["reference_type"] = reference_type_from_bounds(results_hw)

# After collapsing the routing-policy folder out of the filename, the
# (filename, storage_policy)->row mapping must be unique or the merge fans out.
dup = results_hw.duplicated(subset=["filename"], keep=False)
if dup.any():
    raise ValueError(
        f"[HennWaescher] {dup.sum()} duplicate reference filenames after "
        f"collapsing the path — the merge would fan out. Example:\n"
        f"{results_hw.loc[dup, 'filename'].head().tolist()}"
    )

df_henn = df[df["instance_set"] == instance_set_hw].copy()
df_henn = df_henn.merge(
    right=results_hw[["filename", "reference_value", "reference_runtime", "reference_type"]],
    how="left",
    left_on="instance_name",
    right_on="filename",
)
coverage_report(df_henn, "HennWaescher")
df_henn = df_henn.dropna(subset=["reference_value"])
df_henn["gap_[%]"] = gap_min(df_henn, "total_distance")

df_henn_vbs = (
    df_henn
    .sort_values(["total_distance", "total_cpu_time"])
    .groupby("instance_name")
    .first()
    .reset_index()
    [["instance_name", "strategy", "total_distance", "total_cpu_time",
      "gap_[%]", "reference_value", "reference_runtime", "reference_type"]]
)
df_henn_vbs["instance_set"] = "HennWaescher"


# =============================================================================
# Muter/Oencan  (two header rows -> skiprows=1; seed is shifted by -1)
# =============================================================================

instance_set_moe = "MuterOencan"
results_moe = read_result_csv(MUTER_OENCAN_RESULTS_PATH, skiprows=1)

results_moe["random seed"] = results_moe["random seed"] - 1
results_moe = results_moe.dropna(
    subset=["number of orders", "capacity", "random seed"]
).copy()

results_moe["filename"] = results_moe.apply(
    lambda row: f"{int(row['number of orders'])}_{int(row['capacity'])}_{int(row['random seed'])}",
    axis=1,
)

results_moe = results_moe[results_moe["policy"] == "optimal"].copy()
results_moe["reference_value"] = results_moe["UB"]
results_moe["reference_runtime"] = results_moe["time [s]"]
results_moe["reference_type"] = reference_type_from_bounds(results_moe)

df_muter_oencan = df[df["instance_set"] == instance_set_moe].copy()
df_muter_oencan = df_muter_oencan.merge(
    right=results_moe[["filename", "reference_value", "reference_runtime", "reference_type"]],
    how="left",
    left_on="instance_name",
    right_on="filename",
)
coverage_report(df_muter_oencan, "MuterOencan")
df_muter_oencan["gap_[%]"] = gap_min(df_muter_oencan, "total_distance")
df_muter_oencan = df_muter_oencan.dropna(subset=["gap_[%]"])

df_muter_oencan_vbs = (
    df_muter_oencan
    .sort_values(["total_distance", "total_cpu_time"])
    .groupby("instance_name")
    .first()
    .reset_index()
    [["instance_name", "strategy", "total_distance", "total_cpu_time",
      "gap_[%]", "reference_value", "reference_runtime", "reference_type"]]
)
df_muter_oencan_vbs["instance_set"] = "MuterOencan"


# =============================================================================
# Bahceci/Oencan  (opt-flag + bounds)
# =============================================================================

instance_set_boe = "BahceciOencan"
results_boe = read_result_csv(BAHCECI_OENCAN_RESULTS_PATH)

results_boe["filename"] = results_boe["filename"].str.removesuffix(".txt")

results_boe = results_boe[results_boe["policy"] == "optimal"].copy()
results_boe["reference_value"] = results_boe["UB"]
results_boe["reference_runtime"] = results_boe["time [s]"]
results_boe["reference_type"] = reference_type_from_bounds(results_boe)

df_bahceci_oencan = df[df["instance_set"] == instance_set_boe].copy()
df_bahceci_oencan = df_bahceci_oencan.merge(
    right=results_boe[["filename", "reference_value", "reference_runtime", "reference_type"]],
    how="left",
    left_on="instance_name",
    right_on="filename",
)
coverage_report(df_bahceci_oencan, "BahceciOencan")
df_bahceci_oencan["gap_[%]"] = gap_min(df_bahceci_oencan, "total_distance")

df_bahceci_oencan_vbs = df_bahceci_oencan.loc[
    df_bahceci_oencan.groupby("instance_name")["total_distance"].idxmin(),
    ["instance_name", "strategy", "total_distance", "total_cpu_time",
     "gap_[%]", "reference_value", "reference_runtime", "reference_type"],
].copy()
df_bahceci_oencan_vbs["instance_set"] = "BahceciOencan"


# =============================================================================
# Kris  (reported feasible solutions only — no bounds, so never proven optimum)
# =============================================================================

instance_set_kris = "Kris"

results_kris_small = parse_solution_dir(KRIS_SMALL_SOLUTIONS_PATH)
results_kris_small["instance_name"] = results_kris_small["instance_name"] + "_small"

results_kris_large = parse_solution_dir(KRIS_LARGE_SOLUTIONS_PATH)
results_kris_large["instance_name"] = results_kris_large["instance_name"] + "_large"

results_kris = pd.concat([results_kris_small, results_kris_large], ignore_index=True)
results_kris["reference_type"] = "reported feasible solution"
results_kris["reference_runtime"] = np.nan

df_kris = df[df["instance_set"] == instance_set_kris].copy()
df_kris = df_kris.merge(right=results_kris, how="left", on="instance_name")
df_kris = df_kris.dropna(subset=["best_total_distance"]).copy()

# Kris is only compared like-for-like on its native objective: on-time rate.
# (Distance/makespan against the Briant et al. reference are not solution-quality
# gaps -- the reference is due-date-aware -- so they are not reported here.)
df_kris_due_date = df_kris[
    (df_kris["on_time_rate"] != "") &
    (df_kris["scheduling_algo"].notna())
].copy()

df_kris_due_date["on_time_rate"] = df_kris_due_date["on_time_rate"].astype(float)

df_kris_due_date["reference_value"] = df_kris_due_date["best_on_time_rate"]
df_kris_due_date["gap_[%]"] = (
    (df_kris_due_date["reference_value"] - df_kris_due_date["on_time_rate"])
    / df_kris_due_date["reference_value"]
) * 100

df_kris_on_time_vbs = (
    df_kris_due_date
    .sort_values(["on_time_rate", "total_cpu_time"])
    .groupby("instance_name")
    .last()
    .reset_index()
    [
        [
            "instance_name", "strategy", "on_time_rate", "total_distance",
            "total_cpu_time", "reference_value", "reference_runtime",
            "reference_type", "gap_[%]",
        ]
    ]
)
df_kris_on_time_vbs["instance_set"] = "Kris"


# =============================================================================
# Final validation table (VBS vs. reference)
#
# One row per instance set on its like-for-like objective: distance for the six
# routing/batching sets, on-time rate for Kris. Gap = (z_vbs - z_ref)/z_ref
# averaged over referenced instances, VBS = best synthesized pipeline per
# instance. z_ref is the value reported by the source paper for that set (see
# Table 1), NOT a surveyed best-known-solution across all of the literature.
# Kris references are reported-feasible (no proven optima), so # Proven Opt. = 0
# and Mean Gap to Opt is "--".
# =============================================================================

vbs_rows = [
    create_row_vbs(df_sprp_vbs),
    create_row_vbs(df_sprp_ss_vbs),
    create_row_vbs(df_bahceci_oencan_vbs),
    create_row_vbs(df_henn_vbs),
    create_row_vbs(df_muter_oencan_vbs),
    create_row_vbs(df_foodmart_vbs),
    create_row_vbs(df_kris_on_time_vbs, objective="on-time rate"),
]

tab_vbs_vs_bks = pd.concat(vbs_rows, ignore_index=True)

# Round only the float columns; keep the count columns as integers.
float_cols = ["gap_to_bks_[%]", "gap_to_opt_[%]"]
tab_vbs_vs_bks[float_cols] = tab_vbs_vs_bks[float_cols].round(3)

# Column order and human-readable headers for output.
COLUMN_ORDER = [
    "instance_set", "objective",
    "n_instances", "n_referenced", "n_proven_opt",
    "gap_to_bks_[%]", "gap_to_opt_[%]",
]
HEADER_MAP = {
    "instance_set": "Instance Set",
    "objective": "Objective",
    "n_instances": r"\# Instances",
    "n_referenced": r"\# Referenced",
    "n_proven_opt": r"\# Proven Opt.",
    "gap_to_bks_[%]": r"Gap to Ref. [\%]",
    "gap_to_opt_[%]": r"Gap to Opt. [\%]",
}

tab_vbs_vs_bks = tab_vbs_vs_bks[COLUMN_ORDER]

print("\n" + "=" * 80)
print(tab_vbs_vs_bks.to_string(index=False))
print("=" * 80 + "\n")

# CSV keeps machine-readable column names.
tab_vbs_vs_bks.to_csv("tab_vbs_vs_bks.csv", index=False)

# LaTeX gets display headers, italic set names, and -- for undefined opt gaps.
tab_tex = tab_vbs_vs_bks.copy()
tab_tex["instance_set"] = tab_tex["instance_set"].apply(lambda x: rf"\textit{{{x}}}")
tab_tex = tab_tex.rename(columns=HEADER_MAP)

tab_tex.to_latex(
    "tab_vbs_vs_bks.tex",
    index=False,
    escape=False,
    na_rep="--",
    column_format="llrrrrr",
    float_format="%.3f",
)


# =============================================================================
# Appendix: VBS winner distribution (context-dependence)
#
# For each set, on its like-for-like objective, count how often each pipeline
# was the per-instance VBS winner. The spread shows that no single pipeline
# dominates across instances/sets, which is the empirical justification for
# context-aware synthesis. This is a separate object from the validation table (a
# distribution, not a single named pipeline).
# =============================================================================

def build_winner_distribution(frames: list[pd.DataFrame]) -> pd.DataFrame:
    parts = []
    for f in frames:
        counts = (
            f.groupby(["instance_set", "strategy"])
            .size()
            .reset_index(name="n_won")
        )
        totals = f.groupby("instance_set")["instance_name"].nunique()
        counts["share_[%]"] = counts.apply(
            lambda r: 100 * r["n_won"] / totals[r["instance_set"]], axis=1
        )
        parts.append(counts)
    out = pd.concat(parts, ignore_index=True)
    out = out.sort_values(
        ["instance_set", "n_won"], ascending=[True, False]
    ).reset_index(drop=True)
    out["share_[%]"] = out["share_[%]"].round(1)
    return out


# Each set on its validation objective: distance for the six, on-time for Kris.
winner_frames = [
    df_sprp_vbs, df_sprp_ss_vbs, df_bahceci_oencan_vbs,
    df_henn_vbs, df_muter_oencan_vbs, df_foodmart_vbs,
    df_kris_on_time_vbs,
]

winner_distribution = build_winner_distribution(winner_frames)

print("\n" + "=" * 80)
print("VBS winner distribution (per-instance winners, by set)")
print(winner_distribution.to_string(index=False))
print("=" * 80 + "\n")

winner_distribution.to_csv("tab_winner_distribution.csv", index=False)
winner_distribution.to_latex(
    "tab_winner_distribution.tex",
    index=False,
    escape=False,
    na_rep="--",
    column_format="llrr",
    float_format="%.1f",
)


# =============================================================================
# Plots
# =============================================================================

sns.set_style("whitegrid", {"axes.grid": False})
sns.set_context("talk")

df_sprp_vbs["instance_set"] = "SPRP"
df_sprp_ss_vbs["instance_set"] = "SPRP-SS"
df_bahceci_oencan_vbs["instance_set"] = "BahceciOencan"
df_henn_vbs["instance_set"] = "HennWaescher"
df_muter_oencan_vbs["instance_set"] = "MuterOencan"
df_foodmart_vbs["instance_set"] = "Foodmart"


# Kris is on-time rate (different objective/scale), so it is excluded from the
# distance gap-to-reference boxplot to keep the figure on comparable units.
all_gaps = pd.concat([
    df_muter_oencan_vbs[["instance_set", "gap_[%]"]],
    df_bahceci_oencan_vbs[["instance_set", "gap_[%]"]],
    df_foodmart_vbs[["instance_set", "gap_[%]"]],
    df_henn_vbs[["instance_set", "gap_[%]"]],
    df_sprp_vbs[["instance_set", "gap_[%]"]],
    df_sprp_ss_vbs[["instance_set", "gap_[%]"]],
], ignore_index=True)

all_gaps = all_gaps.dropna(subset=["gap_[%]"]).copy()

# The boxplot clips negative gaps for readability. Report how many are dropped
# so the figure's data footprint is not silently different from the table's.
n_negative = int((all_gaps["gap_[%]"] < 0).sum())
if n_negative:
    print(
        f"[plot] clipping {n_negative} negative VBS gaps for the boxplot "
        f"(VBS at or below a reported-feasible reference; kept in the table)."
    )
all_gaps = all_gaps[all_gaps["gap_[%]"] >= 0].copy()


problem_groups = {
    "SPRP": ["SPRP", "SPRP-SS"],
    "OBRP": ["BahceciOencan", "HennWaescher", "MuterOencan", "Foodmart"],
}

mean_gaps = all_gaps.groupby("instance_set", observed=True)["gap_[%]"].mean()

turquoise_palette = ["#4DB6AC", "#26A69A", "#80CBC4", "#00897B", "#B2DFDB"]
group_bg_colors = {"SPRP": "#B3D9F2", "OBRP": "#B3D9C8"}
group_label_colors = {"SPRP": "#1565C0", "OBRP": "#2E7D32"}


def plot_vbs_gap(ax, df, groups_to_plot, show_ylabel=True):
    instance_order = []
    for gname in groups_to_plot:
        present = [
            instance_set
            for instance_set in problem_groups[gname]
            if instance_set in mean_gaps.index
        ]
        instance_order.extend(sorted(present, key=lambda x: mean_gaps[x]))

    subset = df[df["instance_set"].isin(instance_order)].copy()
    subset["instance_set"] = pd.Categorical(
        subset["instance_set"], categories=instance_order, ordered=True
    )

    sns.boxplot(
        data=subset,
        x="instance_set",
        y="gap_[%]",
        ax=ax,
        palette=turquoise_palette,
        order=instance_order,
        linewidth=1.5,
        flierprops=dict(marker="o", markersize=4, alpha=0.5),
    )

    for gname in groups_to_plot:
        indices = [
            instance_order.index(instance_set)
            for instance_set in problem_groups[gname]
            if instance_set in instance_order
        ]
        if indices:
            x0, x1 = min(indices) - 0.4, max(indices) + 0.4
            ax.axvspan(x0, x1, alpha=0.35, color=group_bg_colors[gname], zorder=0)
            ax.text(
                (x0 + x1) / 2, 1.05, gname,
                transform=ax.get_xaxis_transform(),
                ha="center", va="top", fontsize=12, fontweight="bold",
                color=group_label_colors[gname],
                bbox=dict(boxstyle="round,pad=0.2", fc="white", ec="none", alpha=0.6),
            )

    ax.set_ylabel("Gap to Reference (%)" if show_ylabel else "")
    ax.set_xticklabels(ax.get_xticklabels(), rotation=45, ha="right")
    ax.set_xlabel("")
    ax.xaxis.label.set_visible(False)


fig, (ax1, ax2) = plt.subplots(
    1, 2, figsize=(14, 6), gridspec_kw={"width_ratios": [1, 2.5]},
)
plot_vbs_gap(ax1, all_gaps, ["SPRP"], show_ylabel=True)
plot_vbs_gap(ax2, all_gaps, ["OBRP"], show_ylabel=False)

plt.tight_layout()
Path("./plots").mkdir(parents=True, exist_ok=True)
plt.savefig(
    "./plots/vbs_gap_to_bks_boxplot_combined.png",
    dpi=200, bbox_inches="tight", pad_inches=0,
)
plt.show()