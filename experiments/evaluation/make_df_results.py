from pathlib import Path

import pandas as pd

from eval_commons import load_summary_jsons_fast, create_summary_dataframe


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

BASE_PATH = Path("../output")
CACHE_PATH = Path("./df_results.pkl")

SETS_TO_LOAD = [
    "SPRP",
    "SPRP-SS",
    "BahceciOencan",
    "HennWaescherUniform",
    "HennWaescherClassBased",
    "MuterOencan",
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
    "FoodmartData": "OBRP",
    "IOPVRP": "OBSRP",
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
    parts = [
        str(row[col])
        for col in ALGO_COLS
        if pd.notna(row[col]) and row[col] != ""
    ]
    return "+".join(parts)


def load_existing_cache(cache_path: Path) -> pd.DataFrame:
    if cache_path.exists():
        print(f"Reading existing cache: {cache_path}")
        return pd.read_pickle(cache_path)

    print(f"No existing cache found at {cache_path}. Starting from an empty dataframe.")
    return pd.DataFrame()


def load_new_results(base_path: Path, sets_to_load: list[str] | None) -> pd.DataFrame:
    if not sets_to_load:
        return pd.DataFrame()

    summary_data = load_summary_jsons_fast(str(base_path), sets_to_load)
    print(f"Loaded {len(summary_data)} summary files")

    if not summary_data:
        return pd.DataFrame()

    return create_summary_dataframe(summary_data)


def merge_reloaded_sets(df_old: pd.DataFrame, df_new: pd.DataFrame) -> pd.DataFrame:
    if df_new.empty:
        return df_old

    if df_old.empty:
        return df_new

    replace_sets = set(df_new["instance_set"].unique())
    df_old = df_old[~df_old["instance_set"].isin(replace_sets)]

    return pd.concat([df_old, df_new], ignore_index=True)


def append_suffix_once(series: pd.Series, suffix: str) -> pd.Series:
    values = series.astype(str)
    return values.where(values.str.endswith(suffix), values + suffix)


def normalize_instance_sets_for_plots(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    mask_cb = df["instance_set"] == "HennWaescherClassBased"
    mask_u = df["instance_set"] == "HennWaescherUniform"
    mask_small = df["instance_set"] == "KrisSmallDataCorrected"
    mask_large = df["instance_set"] == "KrisLargeData"

    df.loc[mask_cb, "instance_name"] = append_suffix_once(df.loc[mask_cb, "instance_name"], "_cb")
    df.loc[mask_u, "instance_name"] = append_suffix_once(df.loc[mask_u, "instance_name"], "_u")
    df.loc[mask_cb | mask_u, "instance_set"] = "HennWaescher"

    df.loc[mask_small, "instance_name"] = append_suffix_once(df.loc[mask_small, "instance_name"], "_small")
    df.loc[mask_large, "instance_name"] = append_suffix_once(df.loc[mask_large, "instance_name"], "_large")
    df.loc[mask_small | mask_large, "instance_set"] = "Kris"

    return df


def postprocess(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    for col in ALGO_COLS:
        df[col] = df[col].replace(SHORT_NAMES)

    df["strategy"] = df.apply(build_strategy, axis=1)
    df["problem_type"] = df["instance_set"].map(INSTANCE_PROBLEM_MAP)
    df["total_cpu_time"] = df["routing_input_time"] + df["total_route_time"]

    missing_problem_type = df[df["problem_type"].isna()]["instance_set"].unique()
    if len(missing_problem_type) > 0:
        raise ValueError(
            "Missing entries in INSTANCE_PROBLEM_MAP for: "
            + ", ".join(map(str, missing_problem_type))
        )

    if NORMALIZE_INSTANCE_SETS_FOR_PLOTS:
        df = normalize_instance_sets_for_plots(df)
        df["problem_type"] = df["instance_set"].map(INSTANCE_PROBLEM_MAP)

    return df


def print_summary(df: pd.DataFrame) -> None:
    if df.empty:
        print("Dataframe is empty.")
        return

    print("\nDataFrame Summary:")
    print(df.groupby("instance_set").agg(
        n_rows=("instance_name", "count"),
        n_instances=("instance_name", "nunique"),
    ))
    print(f"\nShape: {df.shape}")


def main() -> None:
    df_cache = load_existing_cache(CACHE_PATH)
    df_new = load_new_results(BASE_PATH, SETS_TO_LOAD)

    df = merge_reloaded_sets(df_cache, df_new)
    if df.empty:
        raise RuntimeError("No data loaded. Check BASE_PATH, SETS_TO_LOAD, and CACHE_PATH.")

    df = postprocess(df)
    df.to_pickle(CACHE_PATH)

    print_summary(df)
    print(f"\nSaved: {CACHE_PATH.resolve()}")


if __name__ == "__main__":
    main()
