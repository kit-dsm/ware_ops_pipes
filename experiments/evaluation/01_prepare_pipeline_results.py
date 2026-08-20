"""Combine experiment summaries into one Parquet file."""

from pathlib import Path

import numpy as np
import pandas as pd

from result_loader import create_summary_dataframe, load_summary_jsons


EVAL_DIR = Path(__file__).resolve().parent
ROOT = EVAL_DIR.parents[1]
BASE_PATH = EVAL_DIR.parent / "output"
RESULTS_PATH = EVAL_DIR / "df_results.parquet"

SETS_TO_LOAD = [
    "SPRP",
    "SPRP-SS",
    "BahceciOencan",
    "HennWaescherUniform",
    "HennWaescherClassBased",
    "MuterOencanWG",
    "FoodmartData",
    "KrisSmallDataCorrected",
    "KrisLargeData",
]

NORMALIZE_INSTANCE_SETS_FOR_PLOTS = False

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

INSTANCE_PROBLEM_MAP = {
    "SPRP": "SPRP",
    "SPRP-SS": "SPRP",
    "BahceciOencan": "OBRP",
    "HennWaescherUniform": "OBRP",
    "HennWaescherClassBased": "OBRP",
    "HennWaescher": "OBRP",
    "MuterOencan": "OBRP",
    "MuterOencanWG": "OBRP",
    "FoodmartData": "OBRP",
    "KrisSmallDataCorrected": "OBSRP",
    "KrisLargeData": "OBSRP",
    "Kris": "OBSRP",
}

ALGO_COLS = [
    "item_assignment_algo",
    "batching_algo",
    "routing_algo",
    "scheduling_algo",
]


def build_strategy(row: pd.Series) -> str:
    return "+".join(
        str(row[col])
        for col in ALGO_COLS
        if pd.notna(row[col]) and row[col] != ""
    )


def load_results(base_path: Path, sets_to_load: list[str]) -> pd.DataFrame:
    summary_data = load_summary_jsons(str(base_path), sets_to_load)
    print(f"Loaded {len(summary_data)} summary files")

    if not summary_data:
        raise RuntimeError(f"No summary files found below {base_path}")

    df = create_summary_dataframe(summary_data)

    loaded_sets = set(df["instance_set"].dropna().unique())
    missing_sets = [name for name in sets_to_load if name not in loaded_sets]
    if missing_sets:
        raise RuntimeError(
            "Missing result sets below "
            f"{base_path}: {', '.join(missing_sets)}"
        )

    return df


def append_suffix_once(series: pd.Series, suffix: str) -> pd.Series:
    values = series.astype(str)
    return values.where(values.str.endswith(suffix), values + suffix)


def normalize_instance_sets_for_plots(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    mask_cb = df["instance_set"] == "HennWaescherClassBased"
    mask_u = df["instance_set"] == "HennWaescherUniform"
    mask_small = df["instance_set"] == "KrisSmallDataCorrected"
    mask_large = df["instance_set"] == "KrisLargeData"

    df.loc[mask_cb, "instance_name"] = append_suffix_once(
        df.loc[mask_cb, "instance_name"], "_cb"
    )
    df.loc[mask_u, "instance_name"] = append_suffix_once(
        df.loc[mask_u, "instance_name"], "_u"
    )
    df.loc[mask_cb | mask_u, "instance_set"] = "HennWaescher"

    df.loc[mask_small, "instance_name"] = append_suffix_once(
        df.loc[mask_small, "instance_name"], "_small"
    )
    df.loc[mask_large, "instance_name"] = append_suffix_once(
        df.loc[mask_large, "instance_name"], "_large"
    )
    df.loc[mask_small | mask_large, "instance_set"] = "Kris"

    return df


def postprocess(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    for col in ALGO_COLS:
        df[col] = df[col].replace(SHORT_NAMES)

    df["strategy"] = df.apply(build_strategy, axis=1)
    df["problem_type"] = df["instance_set"].map(INSTANCE_PROBLEM_MAP)

    # New timing columns are absent from legacy summaries.
    for stage_time_col in ["ia_time", "scheduling_time"]:
        if stage_time_col not in df.columns:
            df[stage_time_col] = 0.0

    for loader_col in [
        "layout_parse_time",
        "layout_build_time",
        "layout_load_time",
        "layout_cache_hit",
        "instance_parse_time",
        "instance_build_time",
        "instance_load_time",
        "instance_cache_hit",
    ]:
        if loader_col not in df.columns:
            df[loader_col] = np.nan

    df["total_cpu_time"] = (
        df["ia_time"].fillna(0)
        + df["routing_input_time"].fillna(0)
        + df["total_route_time"].fillna(0)
        + df["scheduling_time"].fillna(0)
    )

    missing_problem_type = (
        df.loc[df["problem_type"].isna(), "instance_set"]
        .dropna()
        .unique()
    )
    if len(missing_problem_type):
        raise ValueError(
            "Missing entries in INSTANCE_PROBLEM_MAP for: "
            + ", ".join(map(str, missing_problem_type))
        )

    if NORMALIZE_INSTANCE_SETS_FOR_PLOTS:
        df = normalize_instance_sets_for_plots(df)
        df["problem_type"] = df["instance_set"].map(INSTANCE_PROBLEM_MAP)

    # File loading is parallel, so sort the rows before writing them.
    sort_cols = [
        c for c in [
            "instance_set",
            "instance_name",
            "item_assignment_algo",
            "batching_algo",
            "routing_algo",
            "scheduling_algo",
        ]
        if c in df.columns
    ]
    return df.sort_values(sort_cols, kind="mergesort").reset_index(drop=True)


def print_summary(df: pd.DataFrame) -> None:
    print("\nDataFrame Summary:")
    print(
        df.groupby("instance_set").agg(
            n_rows=("instance_name", "count"),
            n_instances=("instance_name", "nunique"),
        )
    )
    print(f"\nShape: {df.shape}")


def main() -> None:
    df = load_results(BASE_PATH, SETS_TO_LOAD)
    df = postprocess(df)

    RESULTS_PATH.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(RESULTS_PATH, index=False, engine="pyarrow")

    print_summary(df)
    print(f"\nSaved: {RESULTS_PATH.resolve()}")


if __name__ == "__main__":
    main()
