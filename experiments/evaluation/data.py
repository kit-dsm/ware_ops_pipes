"""
Canonical data loading, normalization, and shared constants for CASOP
evaluation scripts.

This module is the single source of truth for:
- short algorithm names
- algo columns and strategy construction
- instance-set normalization and display names
- problem-type mapping
- objective specifications
- the gap tolerance
- regret thresholds
- complete-case dataframe preparation (NO RR-NF exclusion)
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd

# ===========================================================================
# Naming
# ===========================================================================

SHORT_NAMES: dict[str, str] = {
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

ALGO_COLS = [
    "item_assignment_algo",
    "batching_algo",
    "routing_algo",
    "scheduling_algo",
]

# Algorithm identifiers stored in routing_algo that represent integrated
# components rather than standalone routing algorithms.
IAR_ALGOS = {"RatliffRosenthalNF", "RR-NF"}
BR_ALGOS = {"CombinedBatchingRoutingAssigning", "CBR"}

# ===========================================================================
# Instance sets
# ===========================================================================

INSTANCE_ORDER = [
    "SPRP",
    "SPRP-SS",
    "BahceciOencan",
    "HennWaescher",
    "MuterOencan",
    "FoodmartData",
    "Kris",
]

DISTANCE_SETS = [s for s in INSTANCE_ORDER if s != "Kris"]

DISPLAY_INSTANCE_NAMES = {
    "SPRP": "SPRP",
    "SPRP-SS": "SPRP-SS",
    "BahceciOencan": "BahceciOencan",
    "HennWaescher": "HennWaescher",
    "MuterOencan": "MuterOencan",
    "FoodmartData": "Foodmart",
    "Foodmart": "Foodmart",
    "Kris": "Kris",
}

PROBLEM_TYPE_MAP = {
    "SPRP": "SPRP",
    "SPRP-SS": "SPRP",
    "BahceciOencan": "OBRP",
    "HennWaescher": "OBRP",
    "MuterOencan": "OBRP",
    "FoodmartData": "OBRP",
    "Foodmart": "OBRP",
    "Kris": "OBRSP",
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

# ===========================================================================
# Tolerances and thresholds
# ===========================================================================

GAP_TOL = 1e-9
REGRET_THRESHOLDS = (1.0, 5.0, 10.0)

# ===========================================================================
# Objectives
# ===========================================================================


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


# ===========================================================================
# Helpers
# ===========================================================================

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
        if column not in row:
            continue
        value = row.get(column)
        if pd.notna(value) and str(value) != "":
            parts.append(str(value))
    return "+".join(parts)


def normalize_instance_sets(df: pd.DataFrame) -> pd.DataFrame:
    """Merge the two Henn halves and the two Kris subsets, keeping the
    instance names distinct across the merged halves."""
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
            df.loc[mask, "instance_name"], suffix
        )
        df.loc[mask, "instance_set"] = merged_name
    return df


def prepare_df_results(df: pd.DataFrame) -> pd.DataFrame:
    """Apply short names, build strategy, reconstruct runtime if needed,
    and normalize instance sets.

    No pipeline is excluded: RR-NF and CBR are part of the unified portfolio.
    """
    df = df.copy()

    for column in ALGO_COLS:
        if column in df.columns:
            df[column] = df[column].replace(SHORT_NAMES)

    if "strategy" not in df.columns:
        df["strategy"] = df.apply(build_strategy, axis=1)
    else:
        df["strategy"] = df.apply(build_strategy, axis=1)

    if "total_cpu_time" not in df.columns:
        # Reconstruct from per-stage times if available, else from the
        # legacy routing_input_time + total_route_time fallback.
        stage_cols = {"ia_time", "routing_input_time", "total_route_time", "scheduling_time"}
        if stage_cols.issubset(df.columns):
            df["total_cpu_time"] = sum(
                to_numeric_nonempty(df[c]).fillna(0) for c in stage_cols
            )
        elif {"routing_input_time", "total_route_time"}.issubset(df.columns):
            df["total_cpu_time"] = (
                to_numeric_nonempty(df["routing_input_time"]).fillna(0)
                + to_numeric_nonempty(df["total_route_time"]).fillna(0)
            )
        else:
            raise ValueError(
                "Missing total_cpu_time and the columns required to "
                "reconstruct it: routing_input_time and total_route_time."
            )

    df["total_cpu_time"] = to_numeric_nonempty(df["total_cpu_time"])
    df = normalize_instance_sets(df)
    df["problem_type"] = df["instance_set"].map(PROBLEM_TYPE_MAP)
    return df


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


def latex_escape(text: object) -> str:
    return str(text).replace("_", r"\_")
