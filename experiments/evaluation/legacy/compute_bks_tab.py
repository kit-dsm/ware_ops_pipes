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


# =============================================================================
# Helpers
# =============================================================================

def read_result_csv(path: Path, skiprows=None) -> pd.DataFrame:
    return pd.read_csv(path, sep=";", decimal=",", thousands=".", skiprows=skiprows)


def gap_min(df: pd.DataFrame, obj_col: str, ref_col: str = "reference_value") -> pd.Series:
    return ((df[obj_col] - df[ref_col]) / df[ref_col]) * 100


def gap_max(df: pd.DataFrame, obj_col: str, ref_col: str = "reference_value") -> pd.Series:
    return ((df[ref_col] - df[obj_col]) / df[ref_col]) * 100


def reference_type_from_bounds(df: pd.DataFrame) -> pd.Series:
    return np.where(
        (df["UB"].round(6) == df["LB"].round(6)) & (df["opt?"] == True),
        "optimum",
        "reported feasible solution",
    )


def reference_type_from_lb_ub(df: pd.DataFrame) -> pd.Series:
    return np.where(
        df["UB"].round(6) == df["LB"].round(6),
        "optimum",
        "reported feasible solution",
    )


def create_row_vbs(
    df_vbs: pd.DataFrame,
    kpi_col: str = "total_distance",
    objective: str = "distance",
    ref_col: str = "reference_value",
) -> pd.DataFrame:
    return pd.DataFrame([{
        "instance_set": df_vbs["instance_set"].iloc[0],
        "n instances": df_vbs["instance_name"].nunique(),
        "objective": objective,
        "mean gap": df_vbs["gap_[%]"].mean(),
        "mean runtime VBS (ex-post)": df_vbs["total_cpu_time"].astype(float).mean(),
        "mean objective VBS": df_vbs[kpi_col].astype(float).mean(),
        "mean objective BKS": df_vbs[ref_col].astype(float).mean(),
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


# =============================================================================
# SPRP
# =============================================================================

instance_set_sprp = "SPRP"
results_sprp = read_result_csv(SPRP_RESULTS_PATH)

results_sprp["filename"] = results_sprp.apply(
    lambda row: f"unit_F1_m{row['num aisles']}_C{row['num cells']}_a{row['num articles']}_{row['random seed']}",
    axis=1,
)
results_sprp["reference_value"] = results_sprp["GS MIP cost"]
results_sprp["reference_runtime"] = results_sprp["GS MIP time cplex [ms]"] / 1000
results_sprp["reference_type"] = "optimum"

df_sprp = df[df["instance_set"] == instance_set_sprp].copy()
df_sprp = df_sprp.merge(
    right=results_sprp[["filename", "reference_value", "reference_runtime", "reference_type"]],
    how="left",
    left_on="instance_name",
    right_on="filename",
)
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
# SPRP-SS
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
results_sprp_ss["reference_value"] = results_sprp_ss["GS MIP cost"]
results_sprp_ss["reference_runtime"] = results_sprp_ss["GS MIP time cplex [ms]"] / 1000
results_sprp_ss["reference_type"] = "optimum"

df_sprp_ss = df[df["instance_set"] == instance_set_sprp_ss].copy()
df_sprp_ss = df_sprp_ss.merge(
    right=results_sprp_ss[["filename", "reference_value", "reference_runtime", "reference_type"]],
    how="left",
    left_on="instance_name",
    right_on="filename",
)
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
# Foodmart
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
    how="inner",
    left_on="instance_name",
    right_on="Name",
)
df_foodmart["gap_[%]"] = gap_min(df_foodmart, "total_distance")

df_foodmart_vbs = df_foodmart.loc[
    df_foodmart.groupby("instance_name")["total_distance"].idxmin(),
    ["instance_name", "strategy", "total_distance", "total_cpu_time",
     "gap_[%]", "reference_value", "reference_runtime", "reference_type"],
].copy()
df_foodmart_vbs["instance_set"] = "Foodmart"


# =============================================================================
# Henn/Waescher
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

# BKS comparison: keep feasible solution values, use UB.
results_hw = results_hw[results_hw["policy"] == "optimal"].copy()
results_hw["reference_value"] = results_hw["UB"]
results_hw["reference_runtime"] = results_hw["time [s]"]
results_hw["reference_type"] = reference_type_from_bounds(results_hw)

df_henn = df[df["instance_set"] == instance_set_hw].copy()
df_henn = df_henn.merge(
    right=results_hw[["filename", "reference_value", "reference_runtime", "reference_type"]],
    how="left",
    left_on="instance_name",
    right_on="filename",
)
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
# Muter/Oencan
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

# BKS comparison: keep feasible solution values, use UB.
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
# Bahceci/Oencan
# =============================================================================

instance_set_boe = "BahceciOencan"
results_boe = read_result_csv(BAHCECI_OENCAN_RESULTS_PATH)

results_boe["filename"] = results_boe["filename"].str.removesuffix(".txt")

# BKS comparison: feasible solution value is UB.
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
df_bahceci_oencan["gap_[%]"] = gap_min(df_bahceci_oencan, "total_distance")

df_bahceci_oencan_vbs = df_bahceci_oencan.loc[
    df_bahceci_oencan.groupby("instance_name")["total_distance"].idxmin(),
    ["instance_name", "strategy", "total_distance", "total_cpu_time",
     "gap_[%]", "reference_value", "reference_runtime", "reference_type"],
].copy()
df_bahceci_oencan_vbs["instance_set"] = "BahceciOencan"


# =============================================================================
# Kris
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

# exactly like notebook: distance-only comparison means no scheduling algo
df_kris_distance = df_kris[
    (df_kris["total_distance"] != 0) &
    (df_kris["scheduling_algo"].isna())
].copy()

df_kris_distance["reference_value"] = df_kris_distance["best_total_distance"]
df_kris_distance["gap_[%]"] = (
    (df_kris_distance["total_distance"] - df_kris_distance["reference_value"])
    / df_kris_distance["reference_value"]
) * 100

df_kris_vbs = df_kris_distance.loc[
    df_kris_distance.groupby("instance_name")["total_distance"].idxmin(),
    [
        "instance_name",
        "strategy",
        "total_distance",
        "total_cpu_time",
        "gap_[%]",
        "reference_value",
        "reference_runtime",
        "reference_type",
    ],
].copy()
df_kris_vbs["instance_set"] = "Kris"


# exactly like notebook: due-date comparison means scheduling algo exists
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
            "instance_name",
            "strategy",
            "on_time_rate",
            "total_distance",
            "total_cpu_time",
            "reference_value",
            "reference_runtime",
            "reference_type",
            "gap_[%]",
        ]
    ]
)
df_kris_on_time_vbs["instance_set"] = "Kris"


# hierarchical objective: first on-time rate, then distance
df_kris_distance_due_date_vbs = (
    df_kris_due_date
    .sort_values(
        ["on_time_rate", "total_distance", "total_cpu_time"],
        ascending=[True, False, False],
    )
    .groupby("instance_name")
    .last()
    .reset_index()
    [
        [
            "instance_name",
            "strategy",
            "on_time_rate",
            "total_distance",
            "total_cpu_time",
            "best_total_distance",
            "reference_runtime",
            "reference_type",
        ]
    ]
)

df_kris_distance_due_date_vbs["reference_value"] = (
    df_kris_distance_due_date_vbs["best_total_distance"]
)

df_kris_distance_due_date_vbs["gap_[%]"] = (
    (
        df_kris_distance_due_date_vbs["total_distance"]
        - df_kris_distance_due_date_vbs["reference_value"]
    )
    / df_kris_distance_due_date_vbs["reference_value"]
) * 100

df_kris_distance_due_date_vbs["instance_set"] = "Kris"


# =============================================================================
# Final BKS/reference table
# =============================================================================

vbs_rows = [
    create_row_vbs(df_sprp_vbs, ref_col="reference_value"),
    create_row_vbs(df_sprp_ss_vbs, ref_col="reference_value"),
    create_row_vbs(df_bahceci_oencan_vbs, ref_col="reference_value"),
    create_row_vbs(df_henn_vbs, ref_col="reference_value"),
    create_row_vbs(df_muter_oencan_vbs, ref_col="reference_value"),
    create_row_vbs(df_foodmart_vbs, ref_col="reference_value"),
    create_row_vbs(df_kris_vbs, ref_col="reference_value"),
    create_row_vbs(
        df_kris_on_time_vbs,
        kpi_col="on_time_rate",
        objective="ontime",
        ref_col="reference_value",
    ),
    create_row_vbs(
        df_kris_distance_due_date_vbs,
        kpi_col="total_distance",
        objective="on-time_distance",
        ref_col="reference_value",
    ),
]

tab_vbs_vs_bks = pd.concat(vbs_rows, ignore_index=True)
tab_vbs_vs_bks = tab_vbs_vs_bks.round(3)

tab_vbs_vs_bks["instance_set"] = tab_vbs_vs_bks["instance_set"].apply(
    lambda x: rf"\textit{{{x}}}"
)

tab_vbs_vs_bks = tab_vbs_vs_bks.rename(columns={
    "instance_set": "Instance Set",
})

print(tab_vbs_vs_bks)

tab_vbs_vs_bks.to_csv("tab_vbs_vs_bks.csv", index=False)
tab_vbs_vs_bks.to_latex(
    "tab_vbs_vs_bks.tex",
    index=False,
    escape=False,
    na_rep="--",
    column_format="lrrrrl",
    float_format="%.3f",
)

sns.set_style("whitegrid", {"axes.grid": False})
sns.set_context("talk")

# =============================================================================
# Labels for BKS comparison
# =============================================================================

df_sprp_vbs["instance_set"] = "SPRP"
df_sprp_ss_vbs["instance_set"] = "SPRP-SS"
df_bahceci_oencan_vbs["instance_set"] = "BahceciOencan"
df_henn_vbs["instance_set"] = "HennWaescher"
df_muter_oencan_vbs["instance_set"] = "MuterOencan"
df_foodmart_vbs["instance_set"] = "Foodmart"

df_kris_vbs["instance_set"] = "Kris (Distance)"
df_kris_on_time_vbs["instance_set"] = "Kris (On-Time Rate)"
df_kris_distance_due_date_vbs["instance_set"] = "Kris (On-Time Rate_Distance)"


# =============================================================================
# Collect VBS gaps to BKS
# =============================================================================

all_gaps = pd.concat([
    df_muter_oencan_vbs[["instance_set", "gap_[%]"]],
    df_bahceci_oencan_vbs[["instance_set", "gap_[%]"]],
    df_foodmart_vbs[["instance_set", "gap_[%]"]],
    df_henn_vbs[["instance_set", "gap_[%]"]],
    df_sprp_vbs[["instance_set", "gap_[%]"]],
    df_sprp_ss_vbs[["instance_set", "gap_[%]"]],
    df_kris_vbs[["instance_set", "gap_[%]"]],
    df_kris_on_time_vbs[["instance_set", "gap_[%]"]],
    df_kris_distance_due_date_vbs[["instance_set", "gap_[%]"]],
], ignore_index=True)

all_gaps = all_gaps.dropna(subset=["gap_[%]"]).copy()
all_gaps = all_gaps[all_gaps["gap_[%]"] >= 0].copy()


# =============================================================================
# Plot configuration
# =============================================================================

problem_groups = {
    "SPRP": [
        "SPRP",
        "SPRP-SS",
    ],
    "OBRP": [
        "BahceciOencan",
        "HennWaescher",
        "MuterOencan",
        "Foodmart",
    ],
    "OBRSP": [
        "Kris (Distance)",
        "Kris (On-Time Rate)",
        "Kris (On-Time Rate_Distance)",
    ],
}

mean_gaps = all_gaps.groupby("instance_set", observed=True)["gap_[%]"].mean()

turquoise_palette = ["#4DB6AC", "#26A69A", "#80CBC4", "#00897B", "#B2DFDB"]
group_bg_colors = {
    "SPRP": "#B3D9F2",
    "OBRP": "#B3D9C8",
    "OBRSP": "#B3E0E6",
}
group_label_colors = {
    "SPRP": "#1565C0",
    "OBRP": "#2E7D32",
    "OBRSP": "#00695C",
}


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
        subset["instance_set"],
        categories=instance_order,
        ordered=True,
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
            ax.axvspan(
                x0,
                x1,
                alpha=0.35,
                color=group_bg_colors[gname],
                zorder=0,
            )
            ax.text(
                (x0 + x1) / 2,
                1.05,
                gname,
                transform=ax.get_xaxis_transform(),
                ha="center",
                va="top",
                fontsize=12,
                fontweight="bold",
                color=group_label_colors[gname],
                bbox=dict(
                    boxstyle="round,pad=0.2",
                    fc="white",
                    ec="none",
                    alpha=0.6,
                ),
            )

    if show_ylabel:
        ax.set_ylabel("Gap to BKS (%)")
    else:
        ax.set_ylabel("")

    ax.set_xticklabels(ax.get_xticklabels(), rotation=45, ha="right")
    ax.set_xlabel("")
    ax.xaxis.label.set_visible(False)


# =============================================================================
# Combined figure
# =============================================================================

fig, (ax1, ax2) = plt.subplots(
    1,
    2,
    figsize=(14, 6),
    gridspec_kw={"width_ratios": [1, 2.5]},
)

plot_vbs_gap(ax1, all_gaps, ["SPRP"], show_ylabel=True)
plot_vbs_gap(ax2, all_gaps, ["OBRP", "OBRSP"], show_ylabel=False)

plt.tight_layout()
plt.savefig(
    "./plots/vbs_gap_to_bks_boxplot_combined.png",
    dpi=200,
    bbox_inches="tight",
    pad_inches=0,
)
plt.show()