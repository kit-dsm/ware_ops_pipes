"""
Portfolio-internal SBS-regret analysis.

For each instance, the virtual best solver (VBS) is the ex-post oracle that
returns the best objective value obtained by any pipeline in the evaluated
portfolio. For a minimized objective,

    regret[p, i] = 100 * (z[p, i] - z[VBS, i]) / z[VBS, i].

For the maximized on-time rate, regret is the absolute difference
z[VBS, i] - z[p, i] in percentage points.

The single best solver (SBS) is the fixed pipeline with the smallest mean
instance-wise regret. Exact ties are resolved by lower mean runtime.

The analysis is descriptive and portfolio-internal. It does not evaluate a
learned selector and does not make population-level probability statements.

Outputs:
    tables/sbs_regret.csv
    tables/sbs_regret.tex
    images/sbs_regret_thresholds.pdf

The table reports mean, empirical 90th-percentile, and maximum observed SBS
regret. The figure reports, for the distance-based instance sets, the share of
instances on which SBS regret exceeds pre-specified thresholds.

At threshold tau, the empirical exceedance share is

    100 * mean(regret[SBS, i] > tau).

This directly quantifies how often deployment of the SBS would incur more than
tau percent regret relative to the oracle VBS within the evaluated portfolio.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

GAP_TOL = 1e-9
REGRET_THRESHOLDS = (0.0, 1.0, 5.0, 10.0)

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

DISTANCE_SETS = [name for name in INSTANCE_ORDER if name != "Kris"]

DISPLAY_INSTANCE_NAMES = {
    "SPRP": "SPRP",
    "SPRP-SS": "SPRP-SS",
    "BahceciOencan": "BahceciOencan",
    "HennWaescher": "HennWaescher",
    "MuterOencan": "MuterOencan",
    "FoodmartData": "Foodmart",
    "Kris": "Kris",
}


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
    "RatliffRosenthalNF": "RR-NF",
    "CombinedBatchingRoutingAssigning": "CBR",
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


@dataclass(frozen=True)
class Objective:
    label: str
    column: str
    maximize: bool = False
    regret_mode: str = "relative"


DISTANCE = Objective("distance", "total_distance")

KRIS_OBJECTIVES = [
    Objective("total picking time", "total_time"),
    Objective("makespan", "makespan"),
    Objective(
        "on-time rate (pp)",
        "on_time_rate",
        maximize=True,
        regret_mode="percentage_points",
    ),
]


# ---------------------------------------------------------------------------
# Data preparation
# ---------------------------------------------------------------------------

def to_numeric_nonempty(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def is_missing_or_empty(series: pd.Series) -> pd.Series:
    text = series.astype(str)
    return series.isna() | text.eq("") | text.eq("None") | text.eq("nan")


def append_suffix_once(series: pd.Series, suffix: str) -> pd.Series:
    values = series.astype(str)
    return values.where(values.str.endswith(suffix), values + suffix)


def build_strategy(row: pd.Series) -> str:
    parts: list[str] = []
    for column in ALGO_COLS:
        value = row.get(column)
        if pd.notna(value) and str(value) != "":
            parts.append(str(value))
    return "+".join(parts)


def normalize_instance_sets(df: pd.DataFrame) -> pd.DataFrame:
    """Merge split benchmark subsets while preserving unique instance names."""
    df = df.copy()
    merges = {
        "HennWaescherClassBased": ("HennWaescher", "_cb"),
        "HennWaescherUniform": ("HennWaescher", "_u"),
        "KrisSmallDataCorrected": ("Kris", "_small"),
        "KrisLargeData": ("Kris", "_large"),
    }

    for raw_name, (merged_name, suffix) in merges.items():
        mask = df["instance_set"] == raw_name
        if not mask.any():
            continue
        df.loc[mask, "instance_name"] = append_suffix_once(
            df.loc[mask, "instance_name"],
            suffix,
        )
        df.loc[mask, "instance_set"] = merged_name

    return df


def prepare_df_results(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # Excluded from the evaluated portfolio.
    df = df[df["routing_algo"] != "RatliffRosenthalNF"].copy()

    for column in ALGO_COLS:
        if column in df.columns:
            df[column] = df[column].replace(SHORT_NAMES)

    df["strategy"] = df.apply(build_strategy, axis=1)

    # Runtime is used only to resolve exact ties in mean SBS regret.
    if "total_cpu_time" not in df.columns:
        required = {"routing_input_time", "total_route_time"}
        if not required.issubset(df.columns):
            raise ValueError(
                "Missing total_cpu_time and the columns required to reconstruct it: "
                "routing_input_time and total_route_time."
            )
        df["total_cpu_time"] = (
            to_numeric_nonempty(df["routing_input_time"]).fillna(0)
            + to_numeric_nonempty(df["total_route_time"]).fillna(0)
        )

    df["total_cpu_time"] = to_numeric_nonempty(df["total_cpu_time"])
    df = normalize_instance_sets(df)

    return df


# ---------------------------------------------------------------------------
# Portfolio and regret calculation
# ---------------------------------------------------------------------------

def prepare_complete_portfolio(
    df: pd.DataFrame,
    objective: Objective,
    *,
    instance_col: str = "instance_name",
    context: str,
) -> pd.DataFrame:
    """
    Keep pipelines with a valid objective value on every retained instance.

    The VBS and SBS are computed over the same complete portfolio. If any
    pipeline has a non-positive value for a relative objective, the affected
    instance is excluded explicitly, preserving the behavior of the original
    analysis script.
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
                f"[selection] {context}/{objective.label}: excluding "
                f"{len(invalid_instances)} invalid instance(s) with non-positive "
                f"objective values: {invalid_instances.astype(str).tolist()}"
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
            f"[selection] {context}/{objective.label}: excluding {n_excluded} "
            f"incomplete pipeline(s); best coverage "
            f"{best_incomplete}/{n_instances}."
        )

    if complete.empty:
        raise ValueError(
            f"{context}/{objective.label}: no pipeline has a result on all "
            f"{n_instances} retained instances."
        )

    return work[work["strategy"].isin(complete)].copy()


def add_instance_regrets(
    df: pd.DataFrame,
    objective: Objective,
    *,
    instance_col: str = "instance_name",
) -> pd.DataFrame:
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

    # Remove numerical noise around zero.
    work.loc[work["regret"].abs() <= GAP_TOL, "regret"] = 0.0
    return work


def regret_matrix(
    work: pd.DataFrame,
    *,
    instance_col: str = "instance_name",
) -> pd.DataFrame:
    """Return an instances-by-pipelines matrix of instance-wise regret."""
    return work.pivot(
        index=instance_col,
        columns="strategy",
        values="regret",
    )


def empirical_quantile_higher(values: pd.Series, probability: float) -> float:
    """Return the smallest observed value not exceeded by the given share."""
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
    """Calculate the SBS and summarize its instance-wise regret distribution."""
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

def analyze_context(
    df: pd.DataFrame,
    objective: Objective,
    instance_set: str,
) -> dict:
    display = DISPLAY_INSTANCE_NAMES.get(instance_set, instance_set)

    complete = prepare_complete_portfolio(
        df,
        objective,
        context=display,
    )
    work = add_instance_regrets(complete, objective)
    matrix = regret_matrix(work)

    if matrix.isna().any().any():
        raise ValueError(
            f"{display}/{objective.label}: incomplete regret matrix after "
            "complete-portfolio filtering."
        )

    mean_runtime = work.groupby("strategy")["total_cpu_time"].mean()
    stats = selection_stats(matrix, mean_runtime)

    return {
        "Instance Set": display,
        "Objective": objective.label,
        **stats,
    }


def kris_frame(
    df: pd.DataFrame,
    *,
    common_instances: bool = True,
) -> pd.DataFrame:
    """Keep scheduled Kris pipelines and, optionally, one common instance set."""
    kris = df[df["instance_set"] == "Kris"].copy()
    if kris.empty:
        return kris

    if "scheduling_algo" in kris.columns:
        kris = kris[~is_missing_or_empty(kris["scheduling_algo"])]

    if common_instances:
        shared: pd.Index | None = None
        for objective in KRIS_OBJECTIVES:
            if objective.column not in kris.columns:
                continue
            available = pd.Index(
                kris.loc[
                    to_numeric_nonempty(kris[objective.column]).notna(),
                    "instance_name",
                ].unique()
            )
            shared = available if shared is None else shared.intersection(available)

        if shared is not None:
            dropped = kris["instance_name"].nunique() - len(shared)
            if dropped:
                print(
                    f"[selection] Kris: excluding {dropped} instance(s) without "
                    "values for every reported objective."
                )
            kris = kris[kris["instance_name"].isin(shared)]

    return kris


def build_analysis(df: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict] = []

    for instance_set in DISTANCE_SETS:
        subset = df[df["instance_set"] == instance_set].copy()
        if subset.empty:
            print(f"[selection] no rows for {instance_set}; skipped.")
            continue

        if "scheduling_algo" in subset.columns:
            subset = subset[is_missing_or_empty(subset["scheduling_algo"])]

        rows.append(analyze_context(subset, DISTANCE, instance_set))

    kris = kris_frame(df, common_instances=True)
    if not kris.empty:
        for objective in KRIS_OBJECTIVES:
            if objective.column not in kris.columns:
                print(
                    f"[selection] Kris/{objective.label}: "
                    f"missing column {objective.column}; skipped."
                )
                continue

            rows.append(analyze_context(kris, objective, "Kris"))

    summary = pd.DataFrame(rows)
    order = {
        DISPLAY_INSTANCE_NAMES[name]: index
        for index, name in enumerate(INSTANCE_ORDER)
    }
    objective_order = {
        "distance": 0,
        "total picking time": 1,
        "makespan": 2,
        "on-time rate (pp)": 3,
    }

    summary["_set_order"] = summary["Instance Set"].map(order)
    summary["_objective_order"] = summary["Objective"].map(objective_order)

    return (
        summary.sort_values(
            ["_set_order", "_objective_order"],
            kind="mergesort",
        )
        .drop(columns=["_set_order", "_objective_order"])
        .reset_index(drop=True)
    )


# ---------------------------------------------------------------------------
# Output table
# ---------------------------------------------------------------------------

def latex_escape(text: object) -> str:
    return str(text).replace("_", r"\_")


def make_selection_latex(summary: pd.DataFrame) -> str:
    body: list[str] = []
    previous_set: str | None = None
    row_end = chr(92) * 2

    for _, row in summary.iterrows():
        instance_set = row["Instance Set"]

        first = (
            rf"\textit{{{latex_escape(instance_set)}}}"
            if instance_set != previous_set
            else ""
        )
        previous_set = instance_set

        body.append(
            " & ".join(
                [
                    first,
                    latex_escape(row["Objective"]),
                    latex_escape(row["SBS"]).replace("+", " + "),
                    f"{row['Mean regret']:.2f}",
                    f"{row['p90 regret']:.2f}",
                    f"{row['Max regret']:.2f}",
                    f"{int(row['# Inst.']):,}",
                ]
            )
            + row_end
        )

    return "\n".join(
        [
            r"\begin{table}[t]",
            r"\centering",
            r"\caption{Instance-wise regret of the SBS relative to the oracle VBS. "
            r"Regret is reported in percent for distance and time objectives and in "
            r"percentage points for the on-time rate. The maximum is the largest "
            r"observed regret.}",
            r"\label{tab:selection}",
            r"\small",
            r"\setlength{\tabcolsep}{5pt}",
            r"\begin{tabular}{@{}lllrrrr@{}}",
            r"\toprule",
            r"Instance Set & Objective & SBS & Mean & $p_{90}$ & Max & \# Inst."
            + row_end,
            r"\midrule",
            *body,
            r"\bottomrule",
            r"\end{tabular}",
            r"\end{table}",
            "",
        ]
    )


# ---------------------------------------------------------------------------
# Threshold-risk figure
# ---------------------------------------------------------------------------

def plot_distance_threshold_risk(
    summary: pd.DataFrame,
    output_path: Path,
) -> None:
    """
    Plot the share of instances exceeding selected SBS-regret thresholds.

    Only distance-based rows are shown so all cells use the same objective,
    regret definition, and unit. A cell at threshold tau is the percentage of
    retained instances for which the SBS is more than tau percent worse than
    the oracle VBS.
    """
    plot = summary[summary["Objective"] == DISTANCE.label].copy()
    if plot.empty:
        raise ValueError("No distance-based SBS-regret rows available for plotting.")

    set_order = [
        DISPLAY_INSTANCE_NAMES[name]
        for name in DISTANCE_SETS
        if DISPLAY_INSTANCE_NAMES[name] in set(plot["Instance Set"])
    ]
    plot["Instance Set"] = pd.Categorical(
        plot["Instance Set"],
        categories=set_order,
        ordered=True,
    )
    plot = plot.sort_values("Instance Set", kind="mergesort").reset_index(drop=True)

    columns = [threshold_column(t) for t in REGRET_THRESHOLDS]
    matrix = plot[columns].to_numpy(dtype=float)

    fig, ax = plt.subplots(figsize=(7.5, 4.3))
    image = ax.imshow(
        matrix,
        aspect="auto",
        vmin=0,
        vmax=100,
    )

    ax.set_yticks(np.arange(len(plot)))
    ax.set_yticklabels(plot["Instance Set"])
    ax.set_xticks(np.arange(len(REGRET_THRESHOLDS)))
    ax.set_xticklabels(
        [rf"$>{threshold:g}\%$" for threshold in REGRET_THRESHOLDS]
    )
    ax.set_xlabel("SBS-regret threshold")
    ax.set_title("Instances exceeding the SBS-regret threshold")

    for row_index in range(matrix.shape[0]):
        for column_index in range(matrix.shape[1]):
            value = matrix[row_index, column_index]
            ax.text(
                column_index,
                row_index,
                f"{value:.1f}",
                ha="center",
                va="center",
            )

    colorbar = fig.colorbar(image, ax=ax)
    colorbar.set_label("Instances (%)")

    fig.tight_layout()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, dpi=300, bbox_inches="tight")
    plt.close(fig)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Calculate portfolio-internal SBS regret and create the SBS-regret "
            "summary table."
        )
    )
    parser.add_argument(
        "--df-results",
        type=Path,
        default=Path("./df_results.pkl"),
    )
    parser.add_argument(
        "--tables-dir",
        type=Path,
        default=Path("./tables"),
    )
    parser.add_argument(
        "--images-dir",
        type=Path,
        default=Path("./images"),
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    if not args.df_results.exists():
        raise FileNotFoundError(
            f"Result file not found: {args.df_results}"
        )

    args.tables_dir.mkdir(parents=True, exist_ok=True)
    args.images_dir.mkdir(parents=True, exist_ok=True)

    raw = pd.read_pickle(args.df_results)
    df = prepare_df_results(raw)
    summary = build_analysis(df)

    print_columns = [
        "Instance Set",
        "Objective",
        "SBS",
        "Mean regret",
        "p90 regret",
        "Max regret",
        *[threshold_column(t) for t in REGRET_THRESHOLDS],
        "# Inst.",
        "# Pipe.",
    ]
    print("\n" + summary[print_columns].round(2).to_string(index=False) + "\n")

    csv_path = args.tables_dir / "sbs_regret.csv"
    tex_path = args.tables_dir / "sbs_regret.tex"

    summary.to_csv(csv_path, index=False)
    tex_path.write_text(
        make_selection_latex(summary),
        encoding="utf-8",
    )

    figure_path = args.images_dir / "sbs_regret_thresholds.pdf"
    plot_distance_threshold_risk(summary, figure_path)

    print(f"Generated {csv_path}, {tex_path}, and {figure_path}.")


if __name__ == "__main__":
    main()