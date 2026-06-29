from pathlib import Path

import pandas as pd

from eval_commons import load_summary_jsons_fast, create_summary_dataframe


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

SCRIPT_DIR = Path(__file__).resolve().parent
EXPERIMENTS_DIR = SCRIPT_DIR.parent

BASE_PATH = EXPERIMENTS_DIR / "output"
CACHE_PATH = SCRIPT_DIR / "df_results.pkl"

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

# During prototyping, missing instance sets only produce warnings.
# For paper-final runs, set STRICT = True.
STRICT = False

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

STAGES = [
    "item_assignment",
    "batching",
    "routing",
    "scheduling",
]

ALGO_COLS = [f"{stage}_algo" for stage in STAGES]


# ---------------------------------------------------------------------------
# Small helpers
# ---------------------------------------------------------------------------

def is_nonempty(value) -> bool:
    return pd.notna(value) and str(value) not in {"", "None", "nan"}


def short_name(value) -> str:
    if not is_nonempty(value):
        return ""
    return SHORT_NAMES.get(str(value), str(value))


def short_fp(value, n: int = 8) -> str:
    if not is_nonempty(value):
        return ""
    return str(value)[:n]


def build_strategy(row: pd.Series, *, versioned: bool) -> str:
    parts = []

    for stage in STAGES:
        algo_col = f"{stage}_algo"
        fp_col = f"{stage}_algo_fingerprint"

        if algo_col not in row:
            continue

        algo = row[algo_col]
        if not is_nonempty(algo):
            continue

        part = str(algo)

        if versioned:
            fp = short_fp(row.get(fp_col))
            if fp:
                part = f"{part}@{fp}"

        parts.append(part)

    return "+".join(parts)


def append_suffix_once(series: pd.Series, suffix: str) -> pd.Series:
    values = series.astype(str)
    return values.where(values.str.endswith(suffix), values + suffix)


# ---------------------------------------------------------------------------
# Loading
# ---------------------------------------------------------------------------

def load_existing_results(cache_path: Path) -> pd.DataFrame:
    if not cache_path.exists():
        print(f"No existing dataframe found at {cache_path}.")
        return pd.DataFrame()

    print(f"Reading existing dataframe: {cache_path}")
    return pd.read_pickle(cache_path)


def load_new_results(base_path: Path, sets_to_load: list[str]) -> pd.DataFrame:
    frames = []
    missing_sets = []

    for instance_set in sets_to_load:
        summary_data = load_summary_jsons_fast(str(base_path), [instance_set])

        if not summary_data:
            missing_sets.append(instance_set)
            continue

        print(f"Loaded {len(summary_data)} summary files for {instance_set}")
        frames.append(create_summary_dataframe(summary_data))

    if missing_sets:
        msg = "No summaries found for: " + ", ".join(missing_sets)
        if STRICT:
            raise RuntimeError(msg)
        print("WARNING:", msg)

    if not frames:
        return pd.DataFrame()

    return pd.concat(frames, ignore_index=True)


# ---------------------------------------------------------------------------
# Postprocessing
# ---------------------------------------------------------------------------

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


def add_algorithm_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    for stage in STAGES:
        algo_col = f"{stage}_algo"
        raw_col = f"{algo_col}_raw"
        fp_col = f"{stage}_algo_fingerprint"
        fp_short_col = f"{stage}_algo_fp_short"

        if algo_col not in df.columns:
            df[algo_col] = pd.NA

        if raw_col not in df.columns:
            df[raw_col] = df[algo_col]
        else:
            df[raw_col] = df[raw_col].where(df[raw_col].map(is_nonempty), df[algo_col])

        if fp_col not in df.columns:
            df[fp_col] = pd.NA

        df[algo_col] = df[raw_col].map(short_name)
        df[fp_short_col] = df[fp_col].map(short_fp)

    df["strategy"] = df.apply(lambda row: build_strategy(row, versioned=False), axis=1)
    df["strategy_versioned"] = df.apply(lambda row: build_strategy(row, versioned=True), axis=1)

    return df


def add_problem_type(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    df["problem_type"] = df["instance_set"].map(INSTANCE_PROBLEM_MAP)

    missing = df[df["problem_type"].isna()]["instance_set"].dropna().unique()
    if len(missing) > 0:
        msg = "Missing entries in INSTANCE_PROBLEM_MAP for: " + ", ".join(map(str, missing))
        if STRICT:
            raise ValueError(msg)
        print("WARNING:", msg)
        df["problem_type"] = df["problem_type"].fillna("unknown")

    return df


def add_total_cpu_time(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    if {"routing_input_time", "total_route_time"}.issubset(df.columns):
        df["total_cpu_time"] = (
            pd.to_numeric(df["routing_input_time"], errors="coerce").fillna(0.0)
            + pd.to_numeric(df["total_route_time"], errors="coerce").fillna(0.0)
        )
        return df

    stage_time_cols = [
        f"{stage}_time"
        for stage in STAGES
        if f"{stage}_time" in df.columns
    ]

    if stage_time_cols:
        df["total_cpu_time"] = sum(
            pd.to_numeric(df[col], errors="coerce").fillna(0.0)
            for col in stage_time_cols
        )
        return df

    if "total_cpu_time" in df.columns:
        df["total_cpu_time"] = pd.to_numeric(df["total_cpu_time"], errors="coerce")
        return df

    df["total_cpu_time"] = pd.NA
    return df


def add_result_identity(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    def identity(row: pd.Series) -> str:
        instance_set = row.get("instance_set", "")
        instance_name = row.get("instance_name", "")

        pipeline_fp = row.get("pipeline_chain_fingerprint", "")
        if is_nonempty(pipeline_fp):
            return f"{instance_set}|{instance_name}|pipeline={pipeline_fp}"

        # Fallback for old summaries without fingerprints.
        return f"{instance_set}|{instance_name}|strategy={row.get('strategy_versioned', row.get('strategy', ''))}"

    df["_result_identity"] = df.apply(identity, axis=1)
    return df


def postprocess(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    df = df.copy()

    for required in ["instance_set", "instance_name"]:
        if required not in df.columns:
            raise ValueError(f"Missing required column: {required}")

    if NORMALIZE_INSTANCE_SETS_FOR_PLOTS:
        df = normalize_instance_sets_for_plots(df)

    df = add_algorithm_columns(df)
    df = add_problem_type(df)
    df = add_total_cpu_time(df)
    df = add_result_identity(df)

    return df


def merge_results(df_old: pd.DataFrame, df_new: pd.DataFrame) -> pd.DataFrame:
    if df_old.empty:
        df = df_new.copy()
    elif df_new.empty:
        df = df_old.copy()
    else:
        df = pd.concat([df_old, df_new], ignore_index=True)

    if df.empty:
        return df

    df = postprocess(df)

    # New rows win over old cached rows with the same exact result identity.
    df = df.drop_duplicates(subset=["_result_identity"], keep="last")

    return df.reset_index(drop=True)


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------

def print_summary(df: pd.DataFrame) -> None:
    if df.empty:
        print("Dataframe is empty.")
        return

    print("\nDataFrame Summary:")
    print(
        df.groupby("instance_set", dropna=False).agg(
            n_rows=("instance_name", "count"),
            n_instances=("instance_name", "nunique"),
            n_strategies=("strategy", "nunique"),
            n_strategy_versions=("strategy_versioned", "nunique"),
        )
    )
    print(f"\nShape: {df.shape}")

    if "pipeline_chain_fingerprint" in df.columns:
        n_versioned = df["pipeline_chain_fingerprint"].map(is_nonempty).sum()
        print(f"Rows with pipeline_chain_fingerprint: {n_versioned}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    df_old = load_existing_results(CACHE_PATH)
    df_new = load_new_results(BASE_PATH, SETS_TO_LOAD)

    df = merge_results(df_old, df_new)

    if df.empty:
        raise RuntimeError("No data loaded. Check BASE_PATH, SETS_TO_LOAD, and CACHE_PATH.")

    CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    df.to_pickle(CACHE_PATH)

    print_summary(df)
    print(f"\nSaved: {CACHE_PATH.resolve()}")


if __name__ == "__main__":
    main()