"""Plot the Foodmart results by number of orders."""

from __future__ import annotations

import math
import re
from pathlib import Path

import matplotlib.pyplot as plt
from matplotlib.lines import Line2D
import pandas as pd

from data import DISTANCE, prepare_df_results
from metrics import add_instance_regrets


ROOT = Path(__file__).resolve().parents[2]
EVAL_DIR = Path(__file__).resolve().parent
DF_PATH = EVAL_DIR / "df_results.parquet"
OUT_PATH = EVAL_DIR / "figures" / "foodmart_scaling.pdf"

LS_LIMIT = 240.0

# Strategy names in the result table.
SBS = "GIA+LSOrdNrNN+NN"
LARGE_INSTANCE_WINNER = "GIA+SavingsNN+NN"


# Paper-facing names for configured batching methods.
BATCHING_LABELS = {
    # Local search: current configuration names.
    "LSBatchingNNFiFo": "LS(FiFo, NN)",
    "LSBatchingNNFiFoOrderNr": "LS(OrderNrFiFo, NN)",
    "LSBatchingNNDueDate": "LS(DueDate, NN)",
    "LSBatchingRRFiFo": "LS(FiFo, RR)",
    "LSBatchingRROrderNrFiFo": "LS(OrderNrFiFo, RR)",
    "LSBatchingRRDueDate": "LS(DueDate, RR)",
    "LSBatchingSShapeFiFo": "LS(FiFo, SShape)",
    "LSBatchingSShapeFiFoOrderNr": "LS(OrderNrFiFo, SShape)",
    "LSBatchingSShapeDueDate": "LS(DueDate, SShape)",

    # Local-search names occurring in the existing result data.
    "LSOrdNrNN": "LS(OrderNrFiFo, NN)",

    # Clark--Wright / Savings: current configuration names.
    "ClarkAndWrightNN": "Savings(NN)",
    "ClarkAndWrightRR": "Savings(RR)",
    "ClarkAndWrightSShape": "Savings(SShape)",

    # Savings names occurring in the existing result data.
    "SavingsNN": "Savings(NN)",
    "SavingsRR": "Savings(RR)",
    "SavingsSShape": "Savings(SShape)",
}


def _format_strategy(strategy: str) -> str:
    """Convert stored strategy identifiers to the notation used in the paper."""
    components = strategy.split("+")
    components = [BATCHING_LABELS.get(component, component) for component in components]
    return " + ".join(components)


def _extract_n_orders(instance_name: str) -> int | None:
    match = re.search(r"ord(\d+)", instance_name)
    return int(match.group(1)) if match else None


def main() -> None:
    if not DF_PATH.exists():
        raise FileNotFoundError(
            f"Canonical result table not found: {DF_PATH}. "
            "Run 01_prepare_pipeline_results.py first."
        )

    df = pd.read_parquet(DF_PATH)
    df = prepare_df_results(df)

    fm = df[df["instance_set"] == "FoodmartData"].copy()
    fm["n_orders"] = fm["instance_name"].map(_extract_n_orders)
    fm = fm.dropna(subset=["n_orders"])
    fm["n_orders"] = fm["n_orders"].astype(int)

    fm = add_instance_regrets(fm, DISTANCE)

    # Restrict the figure to pipelines that attain the VBS on at least one instance.
    vbs_winners = fm.loc[fm["regret"] == 0.0, "strategy"].unique()
    fm = fm[fm["strategy"].isin(vbs_winners)].copy()

    perf = (
        fm.groupby(["n_orders", "strategy"])
        .agg(
            gap_pct=("regret", "mean"),
            total_cpu_time=("total_cpu_time", "mean"),
        )
        .reset_index()
    )

    highlighted = {SBS, LARGE_INSTANCE_WINNER}
    missing = highlighted - set(perf["strategy"])
    if missing:
        raise ValueError(
            f"Expected highlighted pipelines not found: {sorted(missing)}"
        )

    fig, axes = plt.subplots(
        2,
        1,
        figsize=(8, 6),
        sharex=True,
    )

    # Background: all remaining VBS-winning pipelines.
    other = perf[~perf["strategy"].isin(highlighted)]

    for _, group in other.groupby("strategy"):
        group = group.sort_values("n_orders")

        axes[0].plot(
            group["n_orders"],
            group["gap_pct"],
            color="0.72",
            lw=0.8,
            alpha=0.55,
            zorder=1,
        )

        axes[1].plot(
            group["n_orders"],
            group["total_cpu_time"],
            color="0.72",
            lw=0.8,
            alpha=0.55,
            zorder=1,
        )

    # Highlight SBS and the pipeline that dominates the largest instances.
    highlight_styles = {
        SBS: {"color": "tab:blue"},
        LARGE_INSTANCE_WINNER: {"color": "tab:orange"},
    }

    for strategy, style in highlight_styles.items():
        group = (
            perf[perf["strategy"] == strategy]
            .sort_values("n_orders")
        )

        axes[0].plot(
            group["n_orders"],
            group["gap_pct"],
            marker="o",
            ms=4,
            lw=1.8,
            zorder=3,
            **style,
        )

        axes[1].plot(
            group["n_orders"],
            group["total_cpu_time"],
            marker="o",
            ms=4,
            lw=1.8,
            zorder=3,
            **style,
        )

    # Top panel.
    axes[0].set_ylabel("Mean gap to VBS [%]")
    axes[0].set_xscale("log")
    axes[0].grid(True, alpha=0.25)

    max_gap = perf["gap_pct"].max()
    gap_upper = max(105, 5 * math.ceil(max_gap / 5))
    axes[0].set_ylim(0, gap_upper)

    # Bottom panel.
    axes[1].axhline(
        LS_LIMIT,
        color="0.4",
        ls="--",
        lw=0.8,
        zorder=0,
    )

    axes[1].set_ylabel("Mean CPU time [s]")
    axes[1].set_xlabel("Number of orders")
    axes[1].set_xscale("log")
    axes[1].grid(True, alpha=0.25)

    # Shared legend using the notation from the paper.
    legend_handles = [
        Line2D(
            [0], [0],
            color="tab:blue",
            marker="o",
            lw=1.8,
            ms=4,
            label=_format_strategy(SBS),
        ),
        Line2D(
            [0], [0],
            color="tab:orange",
            marker="o",
            lw=1.8,
            ms=4,
            label=_format_strategy(LARGE_INSTANCE_WINNER),
        ),
        Line2D(
            [0], [0],
            color="0.72",
            lw=0.8,
            label="Other VBS pipelines",
        ),
        Line2D(
            [0], [0],
            color="0.4",
            ls="--",
            lw=0.8,
            label="240 s time limit",
        ),
    ]

    fig.legend(
        handles=legend_handles,
        loc="upper center",
        ncol=4,
        bbox_to_anchor=(0.5, 1.01),
        frameon=False,
        fontsize=8,
    )

    fig.tight_layout(rect=[0, 0, 1, 0.95])

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(
        OUT_PATH,
        dpi=300,
        bbox_inches="tight",
    )
    plt.close(fig)

    print(f"Saved: {OUT_PATH}")


if __name__ == "__main__":
    main()
