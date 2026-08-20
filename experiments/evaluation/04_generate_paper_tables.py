"""Generate the pipeline and SBS--VBS tables."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

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
)
from metrics import (
    add_instance_regrets,
    add_vbs_membership,
    regret_matrix,
    sbs_attainment_share,
    selection_stats,
)
from portfolio import compute_pipeline_results_overview, prepare_complete_portfolio


ROOT = Path(__file__).resolve().parents[2]
EVAL_DIR = Path(__file__).resolve().parent
RESULTS = EVAL_DIR / "df_results.parquet"
TABLES = EVAL_DIR / "tables"

EXPECTED_INSTANCES = {
    "SPRP": 2400,
    "SPRP-SS": 14300,
    "BahceciOencan": 1350,
    "HennWaescher": 5760,
    "MuterOencan": 270,
    "FoodmartData": 144,
    "Kris": 480,
}


def write_text(path: Path, text: str) -> None:
    path.write_text(text, encoding="utf-8")


def validate_instance_coverage(df: pd.DataFrame) -> None:
    actual = df.groupby("instance_set")["instance_name"].nunique().to_dict()
    failures = {
        name: (expected, int(actual.get(name, 0)))
        for name, expected in EXPECTED_INSTANCES.items()
        if int(actual.get(name, 0)) != expected
    }
    if failures:
        details = ", ".join(
            f"{name}: expected {expected}, found {found}"
            for name, (expected, found) in failures.items()
        )
        raise ValueError(f"Result file has incomplete benchmark coverage: {details}")


def generate_pipeline_space(df: pd.DataFrame) -> pd.DataFrame:
    overview = compute_pipeline_results_overview(df)
    rows: list[dict] = []

    for instance_set in INSTANCE_ORDER:
        row = overview.loc[instance_set]
        n_instances = int(row["n_instances"])
        n_pipelines = int(row["n_pipelines"])
        if n_pipelines % n_instances:
            raise ValueError(
                f"{instance_set}: pipeline rows are not constant per instance"
            )
        rows.append(
            {
                "instance_set": DISPLAY_INSTANCE_NAMES.get(instance_set, instance_set),
                "IA": int(row["IA"]),
                "B": int(row["B"]),
                "R": int(row["R"]),
                "IAR": int(row["IAR"]),
                "BR": int(row["BR"]),
                "S": int(row["S"]),
                "pipelines_per_instance": n_pipelines // n_instances,
            }
        )

    table = pd.DataFrame(rows)
    table.to_csv(TABLES / "pipeline_space.csv", index=False)

    lines = [
        r"\begin{table}[tbp]",
        r"\centering",
        r"\caption{Applicable components and evaluated pipelines by instance set.}",
        r"\label{tab:pipeline_results}",
        r"\small",
        r"\begin{tabular}{@{}lrrrrrrr@{}}",
        r"\toprule",
        "Instance Set & IA & B & R & IAR & BR & S & Pipelines per instance \\\\",
        r"\midrule",
    ]
    for _, row in table.iterrows():
        lines.append(
            rf"\textit{{{latex_escape(row['instance_set'])}}} & "
            + " & ".join(
                str(int(row[column]))
                for column in ["IA", "B", "R", "IAR", "BR", "S", "pipelines_per_instance"]
            )
            + " \\\\"
        )
    lines += [r"\bottomrule", r"\end{tabular}", r"\end{table}", ""]
    write_text(TABLES / "pipeline_space.tex", "\n".join(lines))
    return table


def selection_row(
    subset: pd.DataFrame,
    *,
    display: str,
    objective,
) -> dict:
    complete = prepare_complete_portfolio(subset, objective, context=display)
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
        "SBS at VBS [%]": round(sbs_attainment_share(work, stats["SBS"]), 1),
    }


def generate_sbs_vbs(df: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict] = []
    for instance_set in DISTANCE_SETS:
        subset = df[df["instance_set"].eq(instance_set)].copy()
        if "scheduling_algo" in subset:
            subset = subset[is_missing_or_empty(subset["scheduling_algo"])]
        rows.append(
            selection_row(
                subset,
                display=DISPLAY_INSTANCE_NAMES.get(instance_set, instance_set),
                objective=DISTANCE,
            )
        )

    kris = kris_frame(df, common_instances=True)
    for objective in KRIS_OBJECTIVES:
        rows.append(selection_row(kris, display="Kris", objective=objective))

    table = pd.DataFrame(rows)
    set_order = {
        DISPLAY_INSTANCE_NAMES.get(name, name): index
        for index, name in enumerate(INSTANCE_ORDER)
    }
    objective_order = {
        "distance": 0,
        "total picking time": 1,
        "makespan": 2,
        "on-time rate (pp)": 3,
    }
    table["_set"] = table["Instance Set"].map(set_order)
    table["_objective"] = table["Objective"].map(objective_order)
    table = (
        table.sort_values(["_set", "_objective"], kind="mergesort")
        .drop(columns=["_set", "_objective"])
        .reset_index(drop=True)
    )
    table.to_csv(TABLES / "sbs_vbs.csv", index=False)

    lines = [
        r"\begin{table}[tbp]",
        r"\centering",
        r"\caption{Regret of the SBS relative to the VBS.}",
        r"\label{tab:selection}",
        r"\small",
        r"\begin{tabular}{@{}lllrrr@{}}",
        r"\toprule",
        "Instance Set & Objective & SBS & Mean & $p_{90}$ & Max \\\\",
        r"\midrule",
    ]
    previous_set = None
    for _, row in table.iterrows():
        instance_set = (
            rf"\textit{{{latex_escape(row['Instance Set'])}}}"
            if row["Instance Set"] != previous_set
            else ""
        )
        previous_set = row["Instance Set"]
        strategy = latex_escape(row["SBS"]).replace("+", " + ")
        lines.append(
            f"{instance_set} & {latex_escape(row['Objective'])} & {strategy} & "
            f"{row['Mean regret']:.2f} & {row['p90 regret']:.2f} & "
            f"{row['Max regret']:.2f} \\\\"
        )
    lines += [r"\bottomrule", r"\end{tabular}", r"\end{table}", ""]
    write_text(TABLES / "sbs_vbs.tex", "\n".join(lines))
    return table


def main() -> None:
    if not RESULTS.exists():
        raise FileNotFoundError(
            f"Canonical result table not found: {RESULTS}. "
            "Run 01_prepare_pipeline_results.py first."
        )

    TABLES.mkdir(parents=True, exist_ok=True)
    df = prepare_df_results(pd.read_parquet(RESULTS))
    validate_instance_coverage(df)

    pipeline_space = generate_pipeline_space(df)
    sbs_vbs = generate_sbs_vbs(df)
    print("\nPipeline-space table")
    print(pipeline_space.to_string(index=False))
    print("\nSBS--VBS table")
    print(sbs_vbs.to_string(index=False))
    print(f"\nPaper tables written to {TABLES}")


if __name__ == "__main__":
    main()
