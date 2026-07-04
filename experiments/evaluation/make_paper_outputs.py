from __future__ import annotations

import argparse
from pathlib import Path
from typing import Iterable

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np
import pandas as pd
import seaborn as sns


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

PIPELINE_INSTANCE_ORDER = [
    "SPRP",
    "SPRP-SS",
    "BahceciOencan",
    "HennWaescher",
    "MuterOencan",
    "FoodmartData",
    "Kris",
]

VBS_INSTANCE_ORDER = [
    "SPRP",
    "SPRP-SS",
    "BahceciOencan",
    "HennWaescher",
    "MuterOencan",
    "FoodmartData",
    "Kris",
]

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

OBJECTIVE_DISPLAY_MAP = {
    "distance": "distance",
    "makespan": "makespan",
    "max_tardiness": r"max\_tardiness",
    "on_time_rate": r"on\_time\_rate",
}

RANK_PALETTE = [
    "#00695C",
    "#E65100",
    "#1565C0",
    "#4DB6AC",
    "#C62828",
    "#6A1B9A",
    "#80CBC4",
    "#F9A825",
    "#00897B",
    "#AD1457",
    "#2E7D32",
    "#5C6BC0",
    "#EF6C00",
    "#00838F",
    "#4E342E",
    "#78909C",
    "#26A69A",
    "#D81B60",
    "#558B2F",
    "#3949AB",
]


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
    df = df.copy()

    mask_cb = df["instance_set"] == "HennWaescherClassBased"
    mask_u = df["instance_set"] == "HennWaescherUniform"
    mask_small = df["instance_set"] == "KrisSmallDataCorrected"
    mask_large = df["instance_set"] == "KrisLargeData"

    if mask_cb.any():
        df.loc[mask_cb, "instance_name"] = append_suffix_once(df.loc[mask_cb, "instance_name"], "_cb")
    if mask_u.any():
        df.loc[mask_u, "instance_name"] = append_suffix_once(df.loc[mask_u, "instance_name"], "_u")
    if (mask_cb | mask_u).any():
        df.loc[mask_cb | mask_u, "instance_set"] = "HennWaescher"

    if mask_small.any():
        df.loc[mask_small, "instance_name"] = append_suffix_once(df.loc[mask_small, "instance_name"], "_small")
    if mask_large.any():
        df.loc[mask_large, "instance_name"] = append_suffix_once(df.loc[mask_large, "instance_name"], "_large")
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

    if "total_cpu_time" not in df.columns:
        if {"routing_input_time", "total_route_time"}.issubset(df.columns):
            df["total_cpu_time"] = (
                pd.to_numeric(df["routing_input_time"], errors="coerce").fillna(0)
                + pd.to_numeric(df["total_route_time"], errors="coerce").fillna(0)
            )
        else:
            raise ValueError(
                "Could not construct total_cpu_time. Either provide it in df_results.pkl "
                "or include routing_input_time and total_route_time."
            )

    df = normalize_instance_sets(df)
    df["problem_type"] = df["instance_set"].map(PROBLEM_TYPE_MAP)

    return df



def write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")



def nonempty_nunique(series: pd.Series) -> int:
    s = series.copy()
    s = s.replace("", np.nan)
    return int(s.dropna().nunique())



def is_missing_or_empty(series: pd.Series) -> pd.Series:
    text = series.astype(str)
    return series.isna() | text.eq("") | text.eq("None") | text.eq("nan")



def to_numeric_nonempty(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


# ---------------------------------------------------------------------------
# Table 1: pipeline counts
# ---------------------------------------------------------------------------


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

    if "BahceciOencan" in overview.index:
        overview.loc["BahceciOencan", "BR"] = 1
        overview.loc["BahceciOencan", "R"] = int(overview.loc["BahceciOencan", "R"]) - 1

    overview = overview[["IA", "R", "B", "BR", "S", "n_instances", "n_pipelines"]]
    return overview



def make_pipeline_count_table_latex(overview: pd.DataFrame) -> str:
    pro = overview.reindex(PIPELINE_INSTANCE_ORDER).copy()

    rows = []
    for idx, row in pro.iterrows():
        display_name = DISPLAY_INSTANCE_NAMES.get(idx, idx)
        latex_name = rf"\textit{{{display_name}}}"
        vals = " & ".join(str(int(v)) if pd.notna(v) else "--" for v in row)
        rows.append(rf"{latex_name} & {vals} \\")

    body = "\n".join(rows)
    total_instances = f"{int(pro['n_instances'].sum()):,}"
    total_pipelines = f"{int(pro['n_pipelines'].sum()):,}"

    latex = rf"""
\begin{{table}}
\caption{{Number of resulting pipelines per instance set.}}
\label{{tab:pipeline_results}}
\begin{{tabular}}{{lrrrrrrr}}
\toprule
  & \multicolumn{{5}}{{c}}{{\# Algorithms}} & \# Instances & \# Pipelines\\
\cmidrule(lr){{2-6}}
instance set & IA & R & B & BR & S &  &  \\
\midrule
{body}
\bottomrule
$\sum$ &  &  &  &  &  & {total_instances} & {total_pipelines}\\
\bottomrule
\end{{tabular}}
\end{{table}}
""".strip()

    return latex


# ---------------------------------------------------------------------------
# Table 2: SBS vs VBS overview
# ---------------------------------------------------------------------------


def compute_sbs_vbs_row(
    df: pd.DataFrame,
    metric_col: str,
    objective_display: str,
    problem_display: str,
    instance_set_display: str,
    maximize: bool = False,
) -> dict:
    work = df.copy()
    work[metric_col] = to_numeric_nonempty(work[metric_col])
    work = work.dropna(subset=[metric_col, "strategy", "instance_name"])

    if work.empty:
        raise ValueError(f"No usable rows left for {instance_set_display} / {metric_col}.")

    by_strategy = (
        work.groupby("strategy", as_index=False)[metric_col]
        .mean()
        .sort_values(
            [metric_col, "strategy"],
            ascending=[not maximize, True],
            kind="mergesort",
        )
        .reset_index(drop=True)
    )
    sbs = by_strategy.iloc[0]
    sbs_strategy = sbs["strategy"]
    sbs_mean = float(sbs[metric_col])

    if maximize:
        vbs_mean = float(work.groupby("instance_name")[metric_col].max().mean())
        rel_gap = ((vbs_mean - sbs_mean) / sbs_mean) * 100 if sbs_mean != 0 else np.nan
    else:
        vbs_mean = float(work.groupby("instance_name")[metric_col].min().mean())
        rel_gap = ((sbs_mean - vbs_mean) / sbs_mean) * 100 if sbs_mean != 0 else np.nan

    return {
        "Problem": problem_display,
        "Instance Set": DISPLAY_INSTANCE_NAMES.get(instance_set_display, instance_set_display),
        "SBS Strategy": sbs_strategy,
        "SBS Mean": sbs_mean,
        "VBS Mean": vbs_mean,
        "Relative Gap %": rel_gap,
        "Objective": objective_display,
    }



def build_vbs_overview_table(df: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict] = []

    distance_sets = [
        "SPRP",
        "SPRP-SS",
        "BahceciOencan",
        "HennWaescher",
        "MuterOencan",
        "FoodmartData",
    ]

    for instance_set in distance_sets:
        subset = df[df["instance_set"] == instance_set].copy()
        subset = subset[to_numeric_nonempty(subset["total_distance"]).notna()]
        subset = subset[to_numeric_nonempty(subset["total_distance"]) != 0]
        if "scheduling_algo" in subset.columns:
            subset = subset[is_missing_or_empty(subset["scheduling_algo"])]

        rows.append(
            compute_sbs_vbs_row(
                subset,
                metric_col="total_distance",
                objective_display="distance",
                problem_display=PROBLEM_TYPE_MAP[instance_set],
                instance_set_display=instance_set,
                maximize=False,
            )
        )

    kris = df[df["instance_set"] == "Kris"].copy()
    if "scheduling_algo" in kris.columns:
        kris = kris[~is_missing_or_empty(kris["scheduling_algo"])]
    if "on_time_rate" in kris.columns:
        kris = kris[to_numeric_nonempty(kris["on_time_rate"]).notna()]

    rows.append(
        compute_sbs_vbs_row(
            kris,
            metric_col="total_distance",
            objective_display="distance",
            problem_display="OBRSP",
            instance_set_display="Kris",
            maximize=False,
        )
    )
    rows.append(
        compute_sbs_vbs_row(
            kris,
            metric_col="makespan",
            objective_display="makespan",
            problem_display="OBRSP",
            instance_set_display="Kris",
            maximize=False,
        )
    )
    rows.append(
        compute_sbs_vbs_row(
            kris,
            metric_col="max_tardiness",
            objective_display="max_tardiness",
            problem_display="OBRSP",
            instance_set_display="Kris",
            maximize=False,
        )
    )
    rows.append(
        compute_sbs_vbs_row(
            kris,
            metric_col="on_time_rate",
            objective_display="on_time_rate",
            problem_display="OBRSP",
            instance_set_display="Kris",
            maximize=True,
        )
    )

    out = pd.DataFrame(rows)

    instance_order_map = {name: i for i, name in enumerate([DISPLAY_INSTANCE_NAMES[s] for s in VBS_INSTANCE_ORDER])}
    objective_order_map = {name: i for i, name in enumerate(["distance", "makespan", "max_tardiness", "on_time_rate"])}

    out["_instance_order"] = out["Instance Set"].map(instance_order_map)
    out["_objective_order"] = out["Objective"].map(objective_order_map)
    out = out.sort_values(["_instance_order", "_objective_order"]).drop(columns=["_instance_order", "_objective_order"])

    return out.reset_index(drop=True)



def make_vbs_overview_latex(df_vbs: pd.DataFrame) -> str:
    tex_df = df_vbs.copy()
    tex_df["Instance Set"] = tex_df["Instance Set"].map(lambda x: rf"\textit{{{x}}}")
    tex_df["Objective"] = tex_df["Objective"].map(lambda x: OBJECTIVE_DISPLAY_MAP.get(x, x))

    float_cols = ["SBS Mean", "VBS Mean", "Relative Gap %"]
    tex_df[float_cols] = tex_df[float_cols].round(2)

    latex_tabular = tex_df.to_latex(
        index=False,
        escape=False,
        na_rep="--",
        column_format="llrrrrl",
        float_format="%.2f",
    )

    latex = rf"""
\begin{{table}}[!h]
\centering
\caption{{VBS overview.}}
\label{{tab:vbs_overview_all}}
\begin{{adjustbox}}{{width=\linewidth,center}}
{latex_tabular}
\end{{adjustbox}}
\end{{table}}
""".strip()

    return latex


# ---------------------------------------------------------------------------
# Figure 1: Foodmart gap to VBS + CPU time by size
# ---------------------------------------------------------------------------


def prepare_foodmart_plot_data(df: pd.DataFrame, top_k: int = 7) -> pd.DataFrame:
    fm = df[df["instance_set"] == "FoodmartData"].copy()
    if fm.empty:
        raise ValueError("No FoodmartData rows found in df_results.pkl.")

    fm["instance_key"] = fm["instance_name"].astype(str).str.replace(r"^instances_|_MAL$", "", regex=True)
    fm["n_orders"] = fm["instance_key"].str.extract(r"ord(\d+)")[0]
    fm["n_orders"] = pd.to_numeric(fm["n_orders"], errors="coerce")

    fm["total_distance"] = to_numeric_nonempty(fm["total_distance"])
    fm["total_cpu_time"] = to_numeric_nonempty(fm["total_cpu_time"])
    fm = fm.dropna(subset=["n_orders", "strategy", "instance_key", "total_distance", "total_cpu_time"])

    if fm.empty:
        raise ValueError("No usable Foodmart rows left after preprocessing.")

    perf = (
        fm.groupby(["n_orders", "strategy"], as_index=False)[["total_distance", "total_cpu_time"]]
        .mean()
    )

    vbs_per_instance = fm.groupby(["n_orders", "instance_key"], as_index=False)["total_distance"].min()
    vbs_mean = vbs_per_instance.groupby("n_orders", as_index=False)["total_distance"].mean().rename(columns={"total_distance": "vbs"})

    perf = perf.merge(vbs_mean, on="n_orders", how="left")
    perf["gap_pct"] = (perf["total_distance"] - perf["vbs"]) / perf["vbs"] * 100

    strategy_order = (
        perf.groupby("strategy", as_index=False)["gap_pct"].mean()
        .sort_values(["gap_pct", "strategy"], ascending=[True, True])
        .head(top_k)["strategy"]
        .tolist()
    )

    perf = perf[perf["strategy"].isin(strategy_order)].copy()
    perf["strategy"] = pd.Categorical(perf["strategy"], categories=strategy_order, ordered=True)
    perf = perf.sort_values(["strategy", "n_orders"])

    return perf



def plot_foodmart_gap_runtime(perf: pd.DataFrame, output_path: Path) -> None:
    sns.set_style("whitegrid", {"axes.grid": False})
    sns.set_context("talk")

    order = sorted(perf["n_orders"].dropna().unique())
    strategies = list(perf["strategy"].cat.categories) if hasattr(perf["strategy"], "cat") else sorted(perf["strategy"].unique())
    palette = RANK_PALETTE[: len(strategies)]

    fig, axes = plt.subplots(2, 1, figsize=(14, 8), sharex=True)
    fig.patch.set_facecolor("white")

    for ax in axes:
        ax.grid(axis="y", alpha=0.3)

    sns.lineplot(
        x="n_orders",
        y="gap_pct",
        hue="strategy",
        data=perf,
        ax=axes[0],
        sort=False,
        palette=palette,
        marker="o",
    )
    axes[0].set_ylabel("Gap to VBS (%)")
    axes[0].set_xlabel("")
    axes[0].set_xticks(order)
    axes[0].set_xticklabels([str(int(x)) for x in order])

    sns.lineplot(
        x="n_orders",
        y="total_cpu_time",
        hue="strategy",
        data=perf,
        ax=axes[1],
        sort=False,
        palette=palette,
        marker="o",
    )
    axes[1].set_ylabel("Mean CPU Time (s)")
    axes[1].set_xlabel("n orders")
    axes[1].set_xticks(order)
    axes[1].set_xticklabels([str(int(x)) for x in order], rotation=45)

    axes[1].legend(title="Pipeline")
    if axes[0].get_legend() is not None:
        axes[0].get_legend().remove()

    plt.tight_layout()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(output_path, dpi=300, bbox_inches="tight", pad_inches=0)
    plt.close(fig)


# ---------------------------------------------------------------------------
# Figure 2: Kris rank consistency across objectives
# ---------------------------------------------------------------------------


def compute_pipeline_ranks(objectives_config: dict[str, dict], pipeline_col: str = "strategy") -> pd.DataFrame:
    rank_records: list[dict] = []

    for obj_name, cfg in objectives_config.items():
        col = cfg["col"]
        ascending = cfg["ascending"]
        df_obj = cfg["df"].copy()
        df_obj[col] = pd.to_numeric(df_obj[col], errors="coerce")
        df_obj = df_obj.dropna(subset=[col, pipeline_col])
        if df_obj.empty:
            continue

        avg_perf = df_obj.groupby(pipeline_col, as_index=False)[col].mean()
        avg_perf = avg_perf.sort_values([col, pipeline_col], ascending=[ascending, True]).reset_index(drop=True)
        avg_perf["rank"] = range(1, len(avg_perf) + 1)

        for _, row in avg_perf.iterrows():
            rank_records.append(
                {
                    "pipeline": row[pipeline_col],
                    "objective": obj_name,
                    "rank": row["rank"],
                }
            )

    return pd.DataFrame(rank_records)



def plot_rank_consistency(
    df_ranks: pd.DataFrame,
    output_path: Path,
    objective_groups: dict[str, list[str]] | None = None,
    top_k: int = 10,
    figsize: tuple[int, int] = (12, 6),
) -> None:
    sns.set_style("whitegrid", {"axes.grid": False})
    sns.set_context("talk", rc={"axes.labelsize": 14, "xtick.labelsize": 14, "ytick.labelsize": 14})

    objectives = list(dict.fromkeys(df_ranks["objective"]))
    df_plot = df_ranks[df_ranks["rank"] <= top_k].copy()

    pipelines = sorted(df_plot["pipeline"].unique())
    color_map = {p: RANK_PALETTE[i % len(RANK_PALETTE)] for i, p in enumerate(pipelines)}

    fig, ax = plt.subplots(figsize=figsize)

    if objective_groups:
        group_colors = ["#e8d0d0", "#d0e8d0", "#d0d0e8"]
        for gi, (group_name, group_objs) in enumerate(objective_groups.items()):
            indices = [objectives.index(o) for o in group_objs if o in objectives]
            if indices:
                x0 = min(indices) - 0.45
                x1 = max(indices) + 0.45
                ax.axvspan(x0, x1, alpha=0.25, color=group_colors[gi % len(group_colors)], zorder=0)
                ax.text((x0 + x1) / 2, 0.3, group_name, ha="center", va="bottom", fontweight="bold", color="#444")

    for _, row in df_plot.iterrows():
        x = objectives.index(row["objective"])
        ax.scatter(
            x,
            row["rank"],
            color=color_map[row["pipeline"]],
            s=100,
            zorder=3,
            edgecolors="black",
            linewidths=0.5,
        )

    for pipeline in pipelines:
        df_p = df_plot[df_plot["pipeline"] == pipeline].copy()
        if len(df_p) < 2:
            continue
        xs = [objectives.index(r["objective"]) for _, r in df_p.iterrows()]
        ys = df_p["rank"].to_numpy()
        order = np.argsort(xs)
        ax.plot(
            np.array(xs)[order],
            np.array(ys)[order],
            color=color_map[pipeline],
            alpha=0.35,
            linewidth=1.2,
            zorder=2,
            linestyle="--",
        )

    ax.set_xticks(range(len(objectives)))
    ax.set_xticklabels(objectives, rotation=35, ha="right")
    ax.set_ylabel("Rank")
    ax.set_yticks(range(1, top_k + 1))
    ax.set_ylim(top_k + 0.5, 0.5)
    ax.set_xlim(-0.6, len(objectives) - 0.4)
    ax.grid(axis="y", alpha=0.2, linestyle="--")

    handles = [mpatches.Patch(color=color_map[p], label=p) for p in pipelines]
    ax.legend(handles=handles, bbox_to_anchor=(1.02, 1), loc="upper left", title="Pipeline", fontsize=10)

    plt.tight_layout()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(output_path, bbox_inches="tight", dpi=300)
    plt.close(fig)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate paper tables and figures from df_results.pkl."
    )
    parser.add_argument("--df-results", type=Path, default=Path("./df_results.pkl"))
    parser.add_argument("--images-dir", type=Path, default=Path("./images"))
    parser.add_argument("--tables-dir", type=Path, default=Path("./tables"))
    parser.add_argument("--top-k-foodmart", type=int, default=7)
    parser.add_argument("--top-k-kris", type=int, default=10)
    return parser.parse_args()



def main() -> None:
    args = parse_args()

    if not args.df_results.exists():
        raise FileNotFoundError(f"df_results file not found: {args.df_results}")

    df_raw = pd.read_pickle(args.df_results)
    df = prepare_df_results(df_raw)

    args.images_dir.mkdir(parents=True, exist_ok=True)
    args.tables_dir.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # Table: pipeline results
    # ------------------------------------------------------------------
    pipeline_overview = compute_pipeline_results_overview(df)
    pipeline_overview_csv = args.tables_dir / "pipeline_results.csv"
    pipeline_overview.to_csv(pipeline_overview_csv)

    pipeline_tex = make_pipeline_count_table_latex(pipeline_overview)
    pipeline_tex_path = args.tables_dir / "pipeline_results.tex"
    write_text(pipeline_tex_path, pipeline_tex)

    # ------------------------------------------------------------------
    # Table: SBS vs VBS overview
    # ------------------------------------------------------------------
    df_vbs = build_vbs_overview_table(df)
    df_vbs_rounded = df_vbs.copy()
    for col in ["SBS Mean", "VBS Mean", "Relative Gap %"]:
        df_vbs_rounded[col] = df_vbs_rounded[col].round(2)
    vbs_csv_path = args.tables_dir / "vbs_overview_all.csv"
    df_vbs_rounded.to_csv(vbs_csv_path, index=False)

    vbs_tex = make_vbs_overview_latex(df_vbs)
    vbs_tex_path = args.tables_dir / "vbs_overview_all.tex"
    write_text(vbs_tex_path, vbs_tex)

    # ------------------------------------------------------------------
    # Figure: Foodmart gap/runtime
    # ------------------------------------------------------------------
    foodmart_perf = prepare_foodmart_plot_data(df, top_k=args.top_k_foodmart)
    foodmart_plot_path = args.images_dir / "foodmart_strategy_vs_vbs_distance_time.png"
    plot_foodmart_gap_runtime(foodmart_perf, foodmart_plot_path)

    # ------------------------------------------------------------------
    # Figure: Kris strategy ranks
    # ------------------------------------------------------------------
    df_kris_due_date = df[df["instance_set"] == "Kris"].copy()
    if "scheduling_algo" in df_kris_due_date.columns:
        df_kris_due_date = df_kris_due_date[~is_missing_or_empty(df_kris_due_date["scheduling_algo"])]
    if "on_time_rate" in df_kris_due_date.columns:
        df_kris_due_date = df_kris_due_date[to_numeric_nonempty(df_kris_due_date["on_time_rate"]).notna()]

    objectives_config = {
        "total_distance": {"col": "total_distance", "df": df_kris_due_date, "ascending": True},
        "makespan": {"col": "makespan", "df": df_kris_due_date, "ascending": True},
        "on_time_rate": {"col": "on_time_rate", "df": df_kris_due_date, "ascending": False},
        "max_tardiness": {"col": "max_tardiness", "df": df_kris_due_date, "ascending": True},
        "max_lateness": {"col": "max_lateness", "df": df_kris_due_date, "ascending": True},
    }
    objective_groups = {
        "Distance": ["total_distance"],
        "Time": ["makespan"],
        "Due Date": ["on_time_rate", "max_tardiness", "max_lateness"],
    }
    df_ranks = compute_pipeline_ranks(objectives_config)
    kris_plot_path = args.images_dir / "kris_strategy_ranks.pdf"
    plot_rank_consistency(
        df_ranks,
        output_path=kris_plot_path,
        objective_groups=objective_groups,
        top_k=args.top_k_kris,
    )

    print("Generated outputs:")
    print(f"  {pipeline_tex_path}")
    print(f"  {pipeline_overview_csv}")
    print(f"  {vbs_tex_path}")
    print(f"  {vbs_csv_path}")
    print(f"  {foodmart_plot_path}")
    print(f"  {kris_plot_path}")


if __name__ == "__main__":
    main()
