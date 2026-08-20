"""
Algorithm selection potential and portfolio diagnostics for CASOP.

Two quantities are computed per portfolio P, both from per-instance relative
gaps rather than from mean objective values, because the sets span two orders
of magnitude in instance size:

    g_{p,i} = (z_{p,i} - z_{VBS,i}) / z_{VBS,i}        (minimization)
    SBS_P   = argmin_{p in P}  mean_i g_{p,i}

    gain_P  = mean_i g_{SBS_P,i}          VBS and gaps taken WITHIN P.
              How much a per-instance selector could still recover inside P.

    loss_P  = mean_i (z_{SBS_P,i} - z_{SBS_full,i}) / z_{SBS_full,i}
              SBS_P against the full-portfolio SBS on the instances where both
              ran. No oracle involved: what a warehouse gives up by not being
              able to run the components P excludes. Zero for P = full.

Portfolios are defined by a predicate over the strategy string, see PORTFOLIOS.
The paper reports the full portfolio against the routing-policy portfolio; the
no-exact portfolio is kept in the CSV for the discussion.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Callable

import numpy as np
import pandas as pd

# Two pipelines count as tied on the objective if their relative gap is within
# this tolerance. Used for the co-winner counts.
TIE_TOL = 1e-6

# Column holding the measured runtime. The experiments report wall-clock time
# per pipeline execution; the cache still carries the older column name.
RUNTIME_COL = "total_cpu_time"


# =============================================================================
# Portfolio definitions
# =============================================================================

# Exact components, matched against whole "+"-separated tokens. SavingsRR is a
# batching heuristic that calls RR as an internal evaluator, so it is not exact
# itself and stays in the no-exact portfolio.
EXACT_TOKENS = {
    "RR",                                # Ratliff/Rosenthal exact routing
    "TSP",                               # exact TSP routing
    "RatliffRosenthalNF",                # integrated exact router, scattered storage
    "RR-NF",
    "CombinedBatchingRoutingAssigning",  # integrated exact batching and routing
    "CBR",
}

# Substrings marking a stage that computes a visit order. Matched INSIDE the
# token, not against the whole token, because batching names embed their
# evaluator: SavingsRR runs the routing DP inside the batching stage even when
# the routing stage itself is a policy.
COMPUTED_ROUTE_MARKERS = ("NN", "RR", "TSP")


def _tokens(strategy: str) -> list[str]:
    return [t.strip() for t in str(strategy).split("+")]


def uses_exact_component(strategy: str) -> bool:
    return any(t in EXACT_TOKENS for t in _tokens(strategy))


def uses_policy_routing_only(strategy: str) -> bool:
    """
    True when no stage computes a visit order, so the picker follows a route
    shape that can be given once and repeated (S-shape, return, midpoint,
    largest gap). Nearest neighbour is cheap to implement but produces an
    arbitrary order, so it is grouped with the computed approaches.
    """
    toks = _tokens(strategy)
    if any(t in EXACT_TOKENS for t in toks):
        return False
    return not any(m in t for t in toks for m in COMPUTED_ROUTE_MARKERS)


@dataclass(frozen=True)
class Portfolio:
    key: str
    label: str
    predicate: Callable[[str], bool]


PORTFOLIOS: tuple[Portfolio, ...] = (
    Portfolio("full", "Full portfolio", lambda s: True),
    Portfolio("noexact", "Without exact", lambda s: not uses_exact_component(s)),
    Portfolio("policy", "Routing policies", uses_policy_routing_only),
)

# Portfolios shown in the manuscript table, in column order.
PAPER_PORTFOLIOS = ("full", "policy")


# =============================================================================
# Core computation
# =============================================================================

def add_relative_gaps(
    df: pd.DataFrame,
    metric_col: str,
    maximize: bool = False,
    instance_col: str = "instance_name",
) -> pd.DataFrame:
    """Attach the per-instance VBS and each pipeline's relative gap to it."""
    work = df.copy()
    work[metric_col] = pd.to_numeric(work[metric_col], errors="coerce")
    work = work.dropna(subset=[metric_col, "strategy", instance_col])
    work = work[work[metric_col] >= 0] if maximize else work[work[metric_col] > 0]
    if work.empty:
        raise ValueError(f"No usable rows for metric {metric_col}.")

    work["vbs_value"] = work.groupby(instance_col)[metric_col].transform(
        "max" if maximize else "min"
    )

    # A VBS of zero makes the relative gap undefined. This happens on the
    # on-time rate when no pipeline meets a single deadline; report it rather
    # than dropping the instances silently.
    degenerate = work.loc[work["vbs_value"] == 0, instance_col].nunique()
    if degenerate:
        print(
            f"[selection] {metric_col}: {degenerate} instances have a VBS of 0 "
            f"and are excluded from the gap"
        )

    denom = work["vbs_value"].replace(0, np.nan)
    if maximize:
        work["rel_gap"] = (work["vbs_value"] - work[metric_col]) / denom
    else:
        work["rel_gap"] = (work[metric_col] - work["vbs_value"]) / denom

    return work.dropna(subset=["rel_gap"])


def selection_summary(
    df: pd.DataFrame,
    metric_col: str,
    maximize: bool = False,
    instance_col: str = "instance_name",
) -> dict:
    """SBS of the given frame, its internal gain, runtime, and co-winner counts."""
    work = add_relative_gaps(df, metric_col, maximize, instance_col)

    mean_gap = work.groupby("strategy")["rel_gap"].mean()
    sbs = mean_gap.idxmin()

    at_vbs = work[work["rel_gap"] <= TIE_TOL]
    n_instances = work[instance_col].nunique()

    # Share of instances on which each pipeline reaches the VBS value, ignoring
    # runtime. Separates worse quality from a lost tie-break.
    vbs_share = (
        at_vbs.groupby("strategy")[instance_col].nunique() / n_instances * 100
    ).sort_values(ascending=False)

    runtime = pd.to_numeric(
        work.loc[work["strategy"] == sbs, RUNTIME_COL], errors="coerce"
    ).mean()

    return {
        "sbs": sbs,
        "gain_[%]": float(mean_gap.min()) * 100,
        "time_[s]": float(runtime) if pd.notna(runtime) else np.nan,
        "n_instances": int(n_instances),
        "n_reaching_vbs": int(at_vbs["strategy"].nunique()),
        "vbs_share_top": float(vbs_share.iloc[0]) if len(vbs_share) else np.nan,
    }


def loss_against(
    df: pd.DataFrame,
    metric_col: str,
    reference_sbs: str,
    portfolio_sbs: str,
    maximize: bool = False,
    instance_col: str = "instance_name",
) -> float:
    """
    Mean relative degradation of portfolio_sbs against reference_sbs on the
    instances where both ran. No oracle involved.
    """
    if portfolio_sbs is None or reference_sbs == portfolio_sbs:
        return 0.0

    work = df.copy()
    work[metric_col] = pd.to_numeric(work[metric_col], errors="coerce")

    ref = work[work["strategy"] == reference_sbs].set_index(instance_col)[metric_col]
    alt = work[work["strategy"] == portfolio_sbs].set_index(instance_col)[metric_col]
    common = ref.index.intersection(alt.index)
    if len(common) == 0:
        return np.nan

    ref, alt = ref.loc[common], alt.loc[common]
    diff = (ref - alt) / ref if maximize else (alt - ref) / ref
    return float(diff.mean()) * 100


def portfolio_rows(
    df: pd.DataFrame,
    set_label: str,
    objective_label: str,
    metric_col: str,
    maximize: bool = False,
) -> list[dict]:
    """One row per portfolio for a given instance set and objective."""
    rows: list[dict] = []
    reference_sbs = None

    for pf in PORTFOLIOS:
        subset = df[df["strategy"].map(pf.predicate)]
        if subset.empty or subset["strategy"].nunique() < 1:
            print(f"[selection] {set_label}/{objective_label}: {pf.key} portfolio empty")
            continue
        try:
            summary = selection_summary(subset, metric_col, maximize)
        except ValueError as exc:
            print(f"[selection] {set_label}/{objective_label}/{pf.key}: {exc}")
            continue

        if pf.key == "full":
            reference_sbs = summary["sbs"]

        rows.append(
            {
                "instance_set": set_label,
                "objective": objective_label,
                "portfolio": pf.key,
                "portfolio_label": pf.label,
                "n_pipelines": int(subset["strategy"].nunique()),
                **summary,
                "loss_[%]": loss_against(
                    df, metric_col, reference_sbs, summary["sbs"], maximize
                ),
            }
        )

    return rows


# =============================================================================
# Table assembly
# =============================================================================

DISTANCE_SETS = [
    ("SPRP", "SPRP"),
    ("SPRP-SS", "SPRP-SS"),
    ("BahceciOencan", "BahceciOencan"),
    ("HennWaescher", "HennWaescher"),
    ("MuterOencan", "MuterOencan"),
    ("FoodmartData", "Foodmart"),
]

# Kris is portfolio-internal here, so several objectives are admissible. Only
# picking time is comparable to the literature; the others show whether the
# objective changes which pipeline leads.
KRIS_OBJECTIVES = [
    ("picking time", "total_time", False),
    ("makespan", "makespan", False),
    ("on-time rate", "on_time_rate", True),
]


def _common_kris_instances(kris: pd.DataFrame) -> pd.Index:
    """
    Instances for which every Kris objective has values. The large runs predate
    the total_time aggregation, so without this the picking-time rows cover
    fewer instances than makespan and on-time rate.
    """
    idx = None
    for _, col, _ in KRIS_OBJECTIVES:
        if col not in kris.columns:
            continue
        have = kris.loc[pd.to_numeric(kris[col], errors="coerce").notna(), "instance_name"]
        idx = pd.Index(have.unique()) if idx is None else idx.intersection(have.unique())
    return idx if idx is not None else pd.Index([])


def build_selection_table(
    df: pd.DataFrame,
    kris_feasible_only: bool = True,
    kris_common_instances: bool = True,
) -> pd.DataFrame:
    rows: list[dict] = []

    for raw, label in DISTANCE_SETS:
        subset = df[df["instance_set"] == raw]
        if subset.empty:
            print(f"[selection] no rows for {label}, skipped")
            continue
        # Distance sets have no scheduling stage.
        if "scheduling_algo" in subset.columns:
            subset = subset[subset["scheduling_algo"].isna()]
        rows += portfolio_rows(subset, label, "distance", "total_distance")

    kris = df[df["instance_set"] == "Kris"]
    if not kris.empty and "scheduling_algo" in kris.columns:
        kris = kris[kris["scheduling_algo"].notna()]
        if kris_feasible_only and "max_lateness" in kris.columns:
            lateness = pd.to_numeric(kris["max_lateness"], errors="coerce")
            kris = kris[lateness <= TIE_TOL]
        if kris_common_instances:
            common = _common_kris_instances(kris)
            dropped = kris["instance_name"].nunique() - len(common)
            if dropped:
                print(
                    f"[selection] Kris: {dropped} instances lack values for all "
                    f"objectives and are excluded so the rows are comparable"
                )
            kris = kris[kris["instance_name"].isin(common)]
        for label, col, maximize in KRIS_OBJECTIVES:
            if col not in kris.columns or kris[col].notna().sum() == 0:
                print(f"[selection] Kris/{label}: column {col} missing, skipped")
                continue
            rows += portfolio_rows(kris, "Kris", label, col, maximize)

    table = pd.DataFrame(rows)
    if not table.empty:
        _warn_on_uneven_coverage(table)
    return table


def _warn_on_uneven_coverage(table: pd.DataFrame) -> None:
    """Flag sets whose objectives or portfolios cover different instance counts."""
    for (iset,), grp in table.groupby(["instance_set"]):
        counts = grp["n_instances"].unique()
        if len(counts) > 1:
            print(
                f"[selection] {iset}: instance counts differ across rows "
                f"({sorted(counts)}); the columns are not directly comparable"
            )


# =============================================================================
# LaTeX output
# =============================================================================

def _fmt(value: float, decimals: int = 2) -> str:
    return "---" if pd.isna(value) else f"{value:.{decimals}f}"


def emit_paper_table(
    table: pd.DataFrame,
    out_path: Path,
    portfolios: tuple[str, ...] = PAPER_PORTFOLIOS,
) -> str:
    """
    The table as it appears in the manuscript: full portfolio against the
    restricted one, with each portfolio's own selection gain. The restricted
    SBS is not shown, since it is described in the text.
    """
    wide = table.pivot_table(
        index=["instance_set", "objective"],
        columns="portfolio",
        values=["sbs", "gain_[%]", "loss_[%]", "time_[s]"],
        aggfunc="first",
    )

    order = [s for _, s in DISTANCE_SETS if s in table["instance_set"].values]
    if "Kris" in table["instance_set"].values:
        order.append("Kris")

    lines = [
        r"\begin{table}[t]",
        r"\centering",
        r"\caption{VBS--SBS gap per portfolio and loss of the restricted "
        r"portfolio. Loss is the mean gap of the restricted SBS to the "
        r"full-portfolio SBS.}",
        r"\label{tab:selection}",
        r"\small",
        r"\setlength{\tabcolsep}{5pt}",
        r"\begin{tabular}{@{}lllrrrrr@{}}",
        r"\toprule",
        r"& & & \multicolumn{2}{c}{Full portfolio}"
        r" & \multicolumn{3}{c}{Routing policies}\\",
        r"\cmidrule(lr){4-5}\cmidrule(lr){6-8}",
        r"Instance Set & Objective & Full-portfolio SBS"
        r" & Gain [\%] & Time [s] & Gain [\%] & Loss [\%] & Time [s]\\",
        r"\midrule",
    ]

    for iset in order:
        block = table[table["instance_set"] == iset]
        objectives = list(dict.fromkeys(block["objective"]))
        if iset == "Kris":
            lines.append(r"\midrule")
        for k, obj in enumerate(objectives):
            row = wide.loc[(iset, obj)]
            if len(objectives) > 1:
                first = rf"\multirow{{{len(objectives)}}}{{*}}{{\textit{{{iset}}}}}" if k == 0 else ""
            else:
                first = rf"\textit{{{iset}}}"
            cells = [
                first,
                obj,
                str(row[("sbs", portfolios[0])]).replace("+", " + "),
                _fmt(row[("gain_[%]", portfolios[0])]),
                _fmt(row[("time_[s]", portfolios[0])]),
                _fmt(row[("gain_[%]", portfolios[1])]),
                _fmt(row[("loss_[%]", portfolios[1])]),
                _fmt(row[("time_[s]", portfolios[1])]),
            ]
            lines.append(" & ".join(cells) + r"\\")

    lines += [r"\bottomrule", r"\end{tabular}", r"\end{table}"]

    tex = "\n".join(lines) + "\n"
    out_path.write_text(tex)
    return tex


def emit_selection_table(df: pd.DataFrame, out_dir: Path = Path(".")) -> pd.DataFrame:
    table = build_selection_table(df)
    if table.empty:
        print("[selection] nothing to report")
        return table

    floats = ["gain_[%]", "loss_[%]", "time_[s]"]
    table[floats] = table[floats].astype(float).round(2)

    print("\n" + "=" * 88)
    print("Algorithm selection potential (per-instance normalized gaps)")
    print(
        table[
            [
                "instance_set", "objective", "portfolio", "n_instances",
                "n_pipelines", "sbs", "gain_[%]", "loss_[%]", "time_[s]",
                "n_reaching_vbs",
            ]
        ].to_string(index=False)
    )
    print("=" * 88 + "\n")

    out_dir.mkdir(parents=True, exist_ok=True)
    table.to_csv(out_dir / "tab_selection.csv", index=False)
    print(emit_paper_table(table, out_dir / "tab_selection.tex"))
    return table


# =============================================================================
# Diagnostics
# =============================================================================

def report_diagnostics(df: pd.DataFrame) -> None:
    """Answers the VERIFY items from the data instead of by inspection."""
    print("\n" + "=" * 88)
    print("Portfolio diagnostics")
    print("=" * 88)

    counts = (
        df.groupby("instance_set")
        .agg(
            n_instances=("instance_name", "nunique"),
            n_pipeline_runs=("instance_name", "count"),
            n_strategies=("strategy", "nunique"),
        )
        .sort_index()
    )
    counts["pipelines_per_instance"] = (
        counts["n_pipeline_runs"] / counts["n_instances"]
    ).round(2)
    print("\n[counts] instance and pipeline totals")
    print(counts.to_string())
    print(
        f"\n  total instances: {counts['n_instances'].sum()}"
        f"   total pipeline runs: {counts['n_pipeline_runs'].sum()}"
    )

    # How many pipelines each portfolio retains per set. If the policy
    # portfolio is thin somewhere, its SBS is close to a single pipeline.
    print("\n[portfolios] pipelines retained per set")
    sizes = []
    for iset, grp in df.groupby("instance_set"):
        strategies = grp["strategy"].unique()
        sizes.append(
            {"instance_set": iset}
            | {pf.key: sum(pf.predicate(s) for s in strategies) for pf in PORTFOLIOS}
        )
    print(pd.DataFrame(sizes).set_index("instance_set").sort_index().to_string())

    stage_cols = [
        c for c in
        ["item_assignment_algo", "batching_algo", "routing_algo", "scheduling_algo"]
        if c in df.columns
    ]
    if stage_cols:
        print("\n[stages] distinct configurations executed per stage")
        print(df.groupby("instance_set")[stage_cols].nunique().sort_index().to_string())

    if "routing_algo" in df.columns:
        nf = df[df["strategy"].str.contains("NF", na=False)]
        if not nf.empty:
            has_ia = nf["item_assignment_algo"].notna() & (nf["item_assignment_algo"] != "")
            print(
                f"\n[integrated router] {len(nf)} runs; "
                f"{int(has_ia.sum())} carry an item-assignment component, "
                f"{int((~has_ia).sum())} do not"
            )


def normalize_instance_sets(df: pd.DataFrame) -> pd.DataFrame:
    """
    Same merging as bks_comparison: the two Henn halves become one set and the
    two Kris subsets become one. Without this the selection table skips
    HennWaescher and Kris, because the raw cache still carries the split names.
    Diagnostics run on the raw frame, since the split is what they show.
    """
    df = df.copy()
    rename = {
        "HennWaescherClassBased": ("HennWaescher", "_cb"),
        "HennWaescherUniform": ("HennWaescher", "_u"),
        "KrisSmallDataCorrected": ("Kris", "_small"),
        "KrisLargeData": ("Kris", "_large"),
    }
    for raw, (merged, suffix) in rename.items():
        mask = df["instance_set"] == raw
        if not mask.any():
            continue
        names = df.loc[mask, "instance_name"].astype(str)
        df.loc[mask, "instance_name"] = names.where(
            names.str.endswith(suffix), names + suffix
        )
        df.loc[mask, "instance_set"] = merged
    return df


def main(cache: Path = Path("./df_results.pkl"), out_dir: Path = Path(".")) -> None:
    df = pd.read_pickle(cache)
    report_diagnostics(df)
    emit_selection_table(normalize_instance_sets(df), out_dir)


if __name__ == "__main__":
    main()