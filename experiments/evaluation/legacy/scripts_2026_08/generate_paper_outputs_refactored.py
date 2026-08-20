"""
Generate the three result tables used in the CASOP manuscript:

    pipeline_space.{tex,csv}   Section 5.2
    validation.{tex,csv}       Section 5.3
    selection.{tex,csv}        Section 5.4

Literature-comparison rule:
    comparison instances = evaluated CASOP instances
                           ∩ instances with a published reference value

This set is fixed before CASOP feasibility is checked. The table reports
Feasible / Instances on that fixed set and the mean relative gap over the
feasible CASOP solutions.
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
    is_missing_or_empty,
    kris_frame,
    latex_escape,
    prepare_df_results,
    to_numeric_nonempty,
)
from portfolio import compute_pipeline_results_overview, prepare_complete_portfolio
from metrics import (
    add_instance_regrets,
    add_vbs_membership,
    regret_matrix,
    selection_stats,
    sbs_attainment_share,
)


TABLES_DIR = Path(__file__).resolve().parent / "tables"
CACHE_PATH = Path(__file__).resolve().parent / "df_results.pkl"
REF_DIR = Path(__file__).resolve().parents[2] / "data" / "results"
NA_STR = "---"


# ---------------------------------------------------------------------------
# I/O
# ---------------------------------------------------------------------------

def write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def write_csv(path: Path, df: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False, encoding="utf-8")


def read_reference_csv(path: Path, *, skiprows=None) -> pd.DataFrame:
    return pd.read_csv(
        path,
        sep=";",
        decimal=",",
        thousands=".",
        skiprows=skiprows,
    )


# ===========================================================================
# Section 5.2: pipeline space
# ===========================================================================

def generate_pipeline_space(df: pd.DataFrame) -> pd.DataFrame:
    table = compute_pipeline_results_overview(df)
    ordered = table.reindex(INSTANCE_ORDER)

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
        r" & $|\mathit{IA}^*|$ & $|\mathit{B}^*|$ & $|\mathit{R}^*|$"
        r" & $|\mathit{IAR}^*|$ & $|\mathit{BR}^*|$ & $|\mathit{S}^*|$"
        r" & Full & Evaluated \\",
        r"\midrule",
    ]

    for instance_set, row in ordered.iterrows():
        name = DISPLAY_INSTANCE_NAMES.get(instance_set, instance_set)
        values = " & ".join(
            str(int(value)) if pd.notna(value) else "--"
            for value in row
        )
        lines.append(rf"\textit{{{name}}} & {values} \\")

    lines += [r"\bottomrule", r"\end{tabular}", r"\end{table}", ""]

    write_text(TABLES_DIR / "pipeline_space.tex", "\n".join(lines))
    write_csv(TABLES_DIR / "pipeline_space.csv", table.reset_index())
    return table


# ===========================================================================
# Section 5.3: literature comparison
# ===========================================================================

def _row_min(df: pd.DataFrame, columns: list[str]) -> pd.Series:
    columns = [column for column in columns if column in df.columns]
    if not columns:
        raise KeyError("No expected literature objective column found.")
    return pd.concat(
        [pd.to_numeric(df[column], errors="coerce") for column in columns],
        axis=1,
    ).min(axis=1)


def _unique_reference(
    df: pd.DataFrame,
    instance_col: str,
    value_col: str = "reference_value",
) -> pd.DataFrame:
    """
    One published feasible reference per instance.

    If the source reports several feasible values for the same minimization
    instance, use the smallest one.
    """
    ref = df[[instance_col, value_col]].rename(
        columns={instance_col: "instance_name", value_col: "reference_value"}
    )
    ref["instance_name"] = ref["instance_name"].astype(str)
    ref["reference_value"] = pd.to_numeric(
        ref["reference_value"], errors="coerce"
    )
    ref = ref.dropna(subset=["reference_value"])
    return (
        ref.groupby("instance_name", as_index=False)["reference_value"]
        .min()
        .reset_index(drop=True)
    )


def _evaluated_names(df: pd.DataFrame, instance_set: str) -> set[str]:
    return set(
        df.loc[df["instance_set"] == instance_set, "instance_name"]
        .dropna()
        .astype(str)
        .unique()
    )


def _comparison_population(
    df: pd.DataFrame,
    instance_set: str,
    reference: pd.DataFrame,
) -> pd.DataFrame:
    """
    Literature references for instances actually represented in the CASOP
    evaluation. This intersection is formed before feasibility is checked.
    """
    evaluated = _evaluated_names(df, instance_set)
    population = reference[reference["instance_name"].isin(evaluated)].copy()

    if population.empty:
        raise ValueError(
            f"{instance_set}: no literature reference matches an evaluated instance."
        )

    print(
        f"  {instance_set:<16}"
        f" evaluated={len(evaluated):>6,}"
        f" source refs={reference['instance_name'].nunique():>6,}"
        f" compared={population['instance_name'].nunique():>6,}"
    )
    return population


def _distance_vbs(df: pd.DataFrame, instance_set: str) -> pd.DataFrame:
    """Best valid CASOP distance solution for each instance."""
    work = df[df["instance_set"] == instance_set].copy()
    work["casop_value"] = to_numeric_nonempty(work["total_distance"])

    # 0 is the result sentinel when routing did not produce a solution.
    work = work[work["casop_value"] > 0].copy()
    if work.empty:
        return pd.DataFrame(columns=["instance_name", "casop_value"])

    work["total_cpu_time"] = to_numeric_nonempty(
        work["total_cpu_time"]
    ).fillna(np.inf)

    return (
        work.sort_values(
            ["casop_value", "total_cpu_time"],
            kind="mergesort",
        )
        .groupby("instance_name", as_index=False)
        .first()
        [["instance_name", "casop_value"]]
    )



def _exclude_empty_henn_source_instance(
    population: pd.DataFrame,
    vbs: pd.DataFrame,
) -> pd.DataFrame:
    """
    Exclude the one known Henn-Waescher source instance that contains no orders.

    It is present by name in the benchmark data, but it is not a meaningful
    optimization instance and therefore must not be counted as a CASOP
    infeasibility. The strict assertion prevents any additional CASOP failure
    from being silently removed.
    """
    feasible_names = set(vbs["instance_name"].astype(str))
    missing = sorted(
        set(population["instance_name"].astype(str)) - feasible_names
    )

    if len(missing) != 1:
        raise ValueError(
            "Expected exactly one known empty-order HennWaescher source "
            f"instance, found {len(missing)}: {missing}"
        )

    excluded = missing[0]
    print(
        "  HennWaescher     excluding empty-order source instance: "
        f"{excluded}"
    )
    return population[population["instance_name"] != excluded].copy()


def _validation_row(
    population: pd.DataFrame,
    vbs: pd.DataFrame,
    *,
    set_label: str,
    objective: str,
) -> dict:
    """
    Left-join CASOP onto the fixed comparison population.

    Hence an infeasible/missing CASOP solution remains in the denominator.
    """
    compared = population.merge(
        vbs,
        on="instance_name",
        how="left",
        validate="one_to_one",
    )
    feasible = compared["casop_value"].notna()

    compared["gap_[%]"] = np.nan
    compared.loc[feasible, "gap_[%]"] = (
        (
            compared.loc[feasible, "casop_value"]
            - compared.loc[feasible, "reference_value"]
        )
        / compared.loc[feasible, "reference_value"]
        * 100.0
    )

    return {
        "Instance Set": set_label,
        "Objective": objective,
        "n_feasible": int(feasible.sum()),
        "n_instances": int(len(compared)),
        "gap_to_ref_[%]": (
            float(compared.loc[feasible, "gap_[%]"].mean())
            if feasible.any()
            else np.nan
        ),
    }


# ----- reference loaders ----------------------------------------------------

def _sprp_reference() -> pd.DataFrame:
    ref = read_reference_csv(REF_DIR / "results_SPRP.csv")
    ref["instance_name"] = ref.apply(
        lambda r: (
            f"unit_F1_m{r['num aisles']}_C{r['num cells']}"
            f"_a{r['num articles']}_{r['random seed']}"
        ),
        axis=1,
    )
    ref["reference_value"] = _row_min(
        ref, ["GS MIP cost", "Netw MIP cost", "DP cost"]
    )
    return _unique_reference(ref, "instance_name")


def _sprp_ss_reference() -> pd.DataFrame:
    ref = read_reference_csv(REF_DIR / "results_SPRP-SS.csv")
    ref["demand"] = np.where(ref["unit demand"], "unit", "varying")
    ref["instance_name"] = ref.apply(
        lambda r: (
            f"{r['demand']}_F{r['alpha']}_m{r['num aisles']}"
            f"_C{r['num cells']}_a{r['num articles']}_{r['random seed']}"
        ),
        axis=1,
    )
    ref["reference_value"] = _row_min(
        ref, ["GS MIP cost", "Netw MIP cost"]
    )
    return _unique_reference(ref, "instance_name")


def _exact_routing_reference(ref: pd.DataFrame) -> pd.DataFrame:
    """
    Published UB for the source's exact-routing OBRP variant.

    In these result files, policy == "optimal" identifies the variant with
    exact picker routing. It does not imply that the complete OBRP was proven
    optimal. The manuscript therefore reports a gap to the published UB.
    """
    exact = ref[ref["policy"] == "optimal"].copy()
    if exact.empty:
        raise ValueError("No exact-routing ('optimal' policy) reference rows found.")

    exact["reference_value"] = pd.to_numeric(exact["UB"], errors="coerce")
    return _unique_reference(exact, "instance_name")


def _bahceci_reference() -> pd.DataFrame:
    ref = read_reference_csv(REF_DIR / "results_BahceciOencan.csv")
    ref["instance_name"] = ref["filename"].str.removesuffix(".txt")
    return _exact_routing_reference(ref)


def _henn_reference() -> pd.DataFrame:
    ref = read_reference_csv(REF_DIR / "results_HennWaescher.csv")
    parts = ref["filename"].str.split("\\", regex=False)

    ref["storage_policy"] = parts.str[0].str.split("_").str[1]
    ref["instance_name"] = parts.str[-1].str.replace(
        ".txt", "", regex=False
    )

    ref.loc[ref["storage_policy"] == "uniform", "instance_name"] += "_u"
    ref.loc[ref["storage_policy"] == "class-based", "instance_name"] += "_cb"

    return _exact_routing_reference(ref)


def _muter_reference() -> pd.DataFrame:
    ref = read_reference_csv(REF_DIR / "results_Muter.csv", skiprows=1)
    ref["random seed"] = pd.to_numeric(
        ref["random seed"], errors="coerce"
    ) - 1
    ref = ref.dropna(
        subset=["number of orders", "capacity", "random seed"]
    ).copy()
    ref["instance_name"] = ref.apply(
        lambda r: (
            f"{int(r['number of orders'])}_"
            f"{int(r['capacity'])}_"
            f"{int(r['random seed'])}"
        ),
        axis=1,
    )
    return _exact_routing_reference(ref)


def _foodmart_reference() -> pd.DataFrame:
    ref = read_reference_csv(REF_DIR / "results_Foodmart.csv")
    ref["instance_name"] = ref["Name"].astype(str)
    ref["reference_value"] = pd.to_numeric(ref["UB"], errors="coerce")
    return _unique_reference(ref, "instance_name")


def _foodmart_frame(df: pd.DataFrame) -> pd.DataFrame:
    work = df[df["instance_set"] == "FoodmartData"].copy()
    work["instance_set"] = "Foodmart"
    work["instance_name"] = work["instance_name"].str.replace(
        r"^instances_|_MAL$", "", regex=True
    )
    return work



def _print_kris_infeasible_diagnostics(
    kris: pd.DataFrame,
    reference: pd.DataFrame,
    vbs: pd.DataFrame,
) -> None:
    """
    Print the Kris instances that the Kris evaluator itself marks infeasible.

    build_kris_vbs keeps one VBS row for every instance, including infeasible
    instances. Therefore infeasibility must be read from vbs["feasible"], not
    inferred from whether an instance is present in vbs.
    """
    if "feasible" not in vbs.columns:
        raise KeyError("Kris VBS has no 'feasible' column.")

    feasible_mask = vbs["feasible"] == True  # noqa: E712
    infeasible_vbs = vbs.loc[~feasible_mask].copy()

    print(
        f"[Kris] due-date-feasible VBS instances: "
        f"{int(feasible_mask.sum())} / {len(vbs)}"
    )

    if infeasible_vbs.empty:
        print("[Kris] no infeasible comparison instances.")
        return

    print(
        f"[Kris] {len(infeasible_vbs)} comparison instance(s) are infeasible "
        "according to max_lateness <= 1e-6:"
    )

    for _, chosen in infeasible_vbs.sort_values("instance_name").iterrows():
        instance_name = str(chosen["instance_name"])

        raw = kris[
            kris["instance_name"].astype(str) == instance_name
        ].copy()

        raw_lateness = (
            pd.to_numeric(raw["max_lateness"], errors="coerce")
            if "max_lateness" in raw.columns
            else pd.Series(dtype=float)
        )

        valid_lateness = raw_lateness.dropna()
        min_lateness = (
            float(valid_lateness.min())
            if not valid_lateness.empty
            else np.nan
        )
        n_raw_feasible = int((valid_lateness <= 1e-6).sum())

        chosen_lateness = pd.to_numeric(
            pd.Series([chosen.get("max_lateness")]), errors="coerce"
        ).iloc[0]
        chosen_relaxation = pd.to_numeric(
            pd.Series([chosen.get("relaxation")]), errors="coerce"
        ).iloc[0]

        if valid_lateness.empty:
            reason = "max_lateness is missing for all candidate pipelines"
        elif min_lateness > 1e-6:
            reason = (
                "every candidate pipeline misses at least one due date "
                f"(best max_lateness={min_lateness:.6g})"
            )
        else:
            reason = (
                "BUG: at least one raw candidate is feasible, but the selected "
                "VBS row is marked infeasible"
            )

        fields = [
            f"raw_rows={len(raw)}",
            f"raw_feasible_rows={n_raw_feasible}",
            (
                f"chosen_max_lateness={chosen_lateness:.6g}"
                if pd.notna(chosen_lateness)
                else "chosen_max_lateness=NaN"
            ),
            (
                f"relaxation={chosen_relaxation:.6g}"
                if pd.notna(chosen_relaxation)
                else "relaxation=NaN"
            ),
        ]

        if "total_time" in chosen.index:
            total_time = pd.to_numeric(
                pd.Series([chosen.get("total_time")]), errors="coerce"
            ).iloc[0]
            fields.append(
                f"chosen_total_time={total_time:.6g}"
                if pd.notna(total_time)
                else "chosen_total_time=NaN"
            )

        if "reference_value" in chosen.index:
            reference_value = pd.to_numeric(
                pd.Series([chosen.get("reference_value")]), errors="coerce"
            ).iloc[0]
            fields.append(
                f"reference={reference_value:.6g}"
                if pd.notna(reference_value)
                else "reference=NaN"
            )

        print(
            f"  - {instance_name}: {reason}; "
            + ", ".join(fields)
        )


def _kris_validation_row(df: pd.DataFrame) -> dict:
    """
    Total picking time among due-date-feasible CASOP solutions.

    Kris reference parsing and feasibility/VBS construction remain in the
    dedicated Kris evaluator; this function only fixes the comparison
    population before those calculations are performed.
    """
    from kris_evaluation import (
        build_kris_reference,
        build_kris_table,
        build_kris_vbs,
    )

    reference = build_kris_reference(
        REF_DIR / "allSolutions" / "solutionssmall",
        REF_DIR / "allSolutions" / "solutionslarge",
        REF_DIR / "briant_results",
    )
    if reference is None or reference.empty:
        raise ValueError("No Kris literature references were loaded.")
    if "instance_name" not in reference.columns:
        raise KeyError("Kris reference must contain 'instance_name'.")

    reference = reference.copy()
    reference["instance_name"] = reference["instance_name"].astype(str)

    evaluated = _evaluated_names(df, "Kris")
    n_source = int(reference["instance_name"].nunique())

    # Literature comparison population = published references intersected
    # with the CASOP-evaluated benchmark population.
    reference = reference[
        reference["instance_name"].isin(evaluated)
    ].copy()
    n_instances = int(reference["instance_name"].nunique())

    if n_instances == 0:
        raise ValueError("Kris: no reference matches an evaluated instance.")

    print(
        f"  {'Kris':<16}"
        f" evaluated={len(evaluated):>6,}"
        f" source refs={n_source:>6,}"
        f" compared={n_instances:>6,}"
    )

    kris = df[
        (df["instance_set"] == "Kris")
        & (df["instance_name"].isin(reference["instance_name"]))
    ].copy()

    result = build_kris_vbs(kris, reference)
    if result is None:
        return {
            "Instance Set": "Kris",
            "Objective": "picking time",
            "n_feasible": 0,
            "n_instances": n_instances,
            "gap_to_ref_[%]": np.nan,
        }

    vbs, _ = result

    _print_kris_infeasible_diagnostics(
        kris=kris,
        reference=reference,
        vbs=vbs,
    )

    result_row = build_kris_table(vbs, n_instances).iloc[0]
    print("[Kris] table summary:", result_row.to_dict())

    # build_kris_table distinguishes due-date feasibility from reference
    # availability. The previous refactor incorrectly used n_referenced as the
    # numerator, which turned 293 / 294 into 294 / 294.
    n_feasible = int(result_row["n_feasible"])
    n_referenced = int(result_row["n_referenced"])

    if n_referenced != n_instances:
        raise ValueError(
            "Kris reference-count mismatch: fixed comparison population has "
            f"{n_instances} instances but build_kris_table reports "
            f"{n_referenced} referenced instances."
        )
    if n_feasible > n_instances:
        raise ValueError(
            f"Kris: {n_feasible} feasible instances > {n_instances} references."
        )

    return {
        "Instance Set": "Kris",
        "Objective": "picking time",
        "n_feasible": n_feasible,
        "n_instances": n_instances,
        "gap_to_ref_[%]": float(result_row["gap_to_ref_[%]"]),
    }


def generate_validation(df: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict] = []

    distance_sources = [
        ("SPRP", "SPRP", _sprp_reference),
        ("SPRP-SS", "SPRP-SS", _sprp_ss_reference),
        ("BahceciOencan", "BahceciOencan", _bahceci_reference),
        ("HennWaescher", "HennWaescher", _henn_reference),
        ("MuterOencan", "MuterOencan", _muter_reference),
    ]

    for instance_set, display, load_reference in distance_sources:
        population = _comparison_population(
            df, instance_set, load_reference()
        )
        vbs = _distance_vbs(df, instance_set)

        if instance_set == "HennWaescher":
            population = _exclude_empty_henn_source_instance(
                population, vbs
            )

        rows.append(
            _validation_row(
                population,
                vbs,
                set_label=display,
                objective="distance",
            )
        )

    foodmart = _foodmart_frame(df)
    rows.append(
        _validation_row(
            _comparison_population(
                foodmart, "Foodmart", _foodmart_reference()
            ),
            _distance_vbs(foodmart, "Foodmart"),
            set_label="Foodmart",
            objective="distance",
        )
    )

    rows.append(_kris_validation_row(df))

    summary = pd.DataFrame(rows)
    order = {
        DISPLAY_INSTANCE_NAMES.get(name, name): i
        for i, name in enumerate(INSTANCE_ORDER)
    }
    summary["_order"] = summary["Instance Set"].map(order)
    summary = (
        summary.sort_values("_order", kind="mergesort")
        .drop(columns="_order")
        .reset_index(drop=True)
    )

    lines = [
        r"\begin{table}[tbp]",
        r"\centering",
        r"\caption{Comparison on instances with solution values reported in the literature.}",
        r"\label{tab:validation}",
        r"\small",
        r"\setlength{\tabcolsep}{8pt}",
        r"\begin{tabular}{@{}llrr@{}}",
        r"\toprule",
        r"Instance Set & Objective & Feasible / Instances & Gap to Ref. [\%] \\",
        r"\midrule",
    ]
    for _, row in summary.iterrows():
        gap = (
            f"{row['gap_to_ref_[%]']:.3f}"
            if pd.notna(row["gap_to_ref_[%]"])
            else NA_STR
        )
        lines.append(
            rf"\textit{{{latex_escape(row['Instance Set'])}}} "
            rf"& {latex_escape(row['Objective'])} "
            rf"& {int(row['n_feasible']):,} / {int(row['n_instances']):,} "
            rf"& {gap} \\"
        )
    lines += [r"\bottomrule", r"\end{tabular}", r"\end{table}", ""]

    write_text(TABLES_DIR / "validation.tex", "\n".join(lines))
    write_csv(TABLES_DIR / "validation.csv", summary)

    print("\n[validation] generated table:")
    print(summary.to_string(index=False))

    return summary


# ===========================================================================
# Section 5.4: SBS versus VBS on the evaluated portfolio
# ===========================================================================

def _selection_row(
    subset: pd.DataFrame,
    *,
    display: str,
    objective,
) -> dict:
    complete = prepare_complete_portfolio(
        subset, objective, context=display
    )
    work = add_instance_regrets(complete, objective)
    work = add_vbs_membership(work)
    matrix = regret_matrix(work)
    mean_runtime = work.groupby("strategy")["total_cpu_time"].mean()
    stats = selection_stats(matrix, mean_runtime)

    return {
        "Instance Set": display,
        "Objective": objective.label,
        "SBS": stats["SBS"],
        "Mean regret": stats["Mean regret"],
        "p90 regret": stats["p90 regret"],
        "Max regret": stats["Max regret"],
        "SBS at VBS [%]": round(
            sbs_attainment_share(work, stats["SBS"]), 1
        ),
    }


def generate_selection(df: pd.DataFrame) -> pd.DataFrame:
    """
    SBS/VBS analysis on the evaluated portfolio, independent of literature
    reference availability.
    """
    rows: list[dict] = []

    for instance_set in DISTANCE_SETS:
        subset = df[df["instance_set"] == instance_set].copy()
        if "scheduling_algo" in subset.columns:
            subset = subset[
                is_missing_or_empty(subset["scheduling_algo"])
            ]
        if subset.empty:
            continue

        rows.append(
            _selection_row(
                subset,
                display=DISPLAY_INSTANCE_NAMES.get(
                    instance_set, instance_set
                ),
                objective=DISTANCE,
            )
        )

    kris = kris_frame(df, common_instances=True)
    for objective in KRIS_OBJECTIVES:
        if kris.empty or objective.column not in kris.columns:
            continue
        rows.append(
            _selection_row(
                kris,
                display="Kris",
                objective=objective,
            )
        )

    summary = pd.DataFrame(rows)

    set_order = {
        DISPLAY_INSTANCE_NAMES.get(name, name): i
        for i, name in enumerate(INSTANCE_ORDER)
    }
    objective_order = {
        "distance": 0,
        "total picking time": 1,
        "makespan": 2,
        "on-time rate (pp)": 3,
    }
    summary["_set"] = summary["Instance Set"].map(set_order)
    summary["_objective"] = summary["Objective"].map(objective_order)
    summary = (
        summary.sort_values(["_set", "_objective"], kind="mergesort")
        .drop(columns=["_set", "_objective"])
        .reset_index(drop=True)
    )

    lines = [
        r"\begin{table}[!t]",
        r"\centering",
        r"\caption{Instance-wise regret of the SBS relative to the oracle VBS "
        r"and the SBS attainment rate per instance set and objective.}",
        r"\label{tab:selection}",
        r"\small",
        r"\setlength{\tabcolsep}{5pt}",
        r"\begin{tabular}{@{}lllrrrr@{}}",
        r"\toprule",
        r"Instance Set & Objective & SBS & Mean & $p_{90}$ & Max & SBS@VBS \\",
        r"\midrule",
    ]

    previous_set = None
    for _, row in summary.iterrows():
        first = (
            rf"\textit{{{latex_escape(row['Instance Set'])}}}"
            if row["Instance Set"] != previous_set
            else ""
        )
        previous_set = row["Instance Set"]
        lines.append(
            f"{first} "
            f"& {latex_escape(row['Objective'])} "
            f"& {latex_escape(row['SBS']).replace('+', ' + ')} "
            f"& {row['Mean regret']:.2f} "
            f"& {row['p90 regret']:.2f} "
            f"& {row['Max regret']:.2f} "
            f"& {row['SBS at VBS [%]']:.0f} \\\\"
        )

    lines += [r"\bottomrule", r"\end{tabular}", r"\end{table}", ""]

    write_text(TABLES_DIR / "selection.tex", "\n".join(lines))
    write_csv(TABLES_DIR / "selection.csv", summary)
    return summary


# ===========================================================================
# Main
# ===========================================================================

def main() -> None:
    global TABLES_DIR

    parser = argparse.ArgumentParser(
        description="Generate CASOP manuscript result tables."
    )
    parser.add_argument("--cache", type=Path, default=CACHE_PATH)
    parser.add_argument("--tables-dir", type=Path, default=TABLES_DIR)
    args = parser.parse_args()

    TABLES_DIR = args.tables_dir
    TABLES_DIR.mkdir(parents=True, exist_ok=True)

    df = pd.read_pickle(args.cache)
    print(f"Loaded {df.shape[0]:,} result rows.")

    df = prepare_df_results(df)

    print("\n--- Section 5.2: pipeline space ---")
    generate_pipeline_space(df)

    print("\n--- Section 5.3: literature comparison ---")
    generate_validation(df)

    print("\n--- Section 5.4: SBS versus VBS ---")
    generate_selection(df)

    print(f"\nGenerated tables in {TABLES_DIR}")


if __name__ == "__main__":
    main()
