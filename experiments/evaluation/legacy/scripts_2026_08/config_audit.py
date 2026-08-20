"""
Configuration-driven portfolio audit.

Resolves the current algorithm portfolio from algorithm cards + configs.yaml,
matches result rows strictly against the current configured portfolio, and
classifies every result row as current, legacy, or missing.

Produces:
  generated/current_portfolio_manifest.csv
  generated/result_configuration_audit.csv
  generated/missing_current_results.csv
"""

from __future__ import annotations

import csv
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any

import pandas as pd
import yaml

from ware_ops_algos.algorithms.algorithm_cards import load_packaged_algo_cards
from ware_ops_algos.domain_algo_mapper.domain_algo_mapper import DomainAlgorithmMapper
from ware_ops_algos.taxonomy.taxonomy import TAXONOMY
from ware_ops_algos.domain_models.datacards import load_and_flatten_data_card

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
EVAL_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = EVAL_DIR.parents[1]
GENERATED_DIR = EVAL_DIR / "generated"
GENERATED_DIR.mkdir(exist_ok=True)

CARDS_DIR = PROJECT_ROOT / "data" / "data_cards"
DF_PATH = EVAL_DIR / "df_results.pkl"

# ---------------------------------------------------------------------------
# Instance-set name mapping (data card file → df instance_set values)
# ---------------------------------------------------------------------------
DATA_CARD_TO_INSTANCE_SETS: dict[str, list[str]] = {
    "sprp": ["SPRP"],
    "sprp_ss": ["SPRP-SS"],
    "bahceci_oencan": ["BahceciOencan"],
    "henn_waescher": ["HennWaescherUniform", "HennWaescherClassBased"],
    "muter_oencan": ["MuterOencan"],
    "foodmart": ["FoodmartData"],
    "kris": ["KrisSmallDataCorrected", "KrisLargeData"],
}

# Display names for merged sets
INSTANCE_SET_DISPLAY = {
    "SPRP": "SPRP",
    "SPRP-SS": "SPRP-SS",
    "BahceciOencan": "BahceciOencan",
    "HennWaescherUniform": "HennWaescher",
    "HennWaescherClassBased": "HennWaescher",
    "MuterOencan": "MuterOencan",
    "FoodmartData": "Foodmart",
    "KrisSmallDataCorrected": "Kris",
    "KrisLargeData": "Kris",
}

# ---------------------------------------------------------------------------
# Short-name mapping (current card algo_name → df display name)
# This mirrors the SHORT_NAMES in make_df_results.py / data.py.
# Only entries that actually change the name are needed.
# ---------------------------------------------------------------------------
SHORT_NAMES: dict[str, str] = {
    "GreedyItemAssignment": "GIA",
    "GreedyIA": "GIA",
    "GreedyPickLocationSelector": "GIA",
    "MinMinItemAssignment": "MinMinIA",
    "MinMaxItemAssignment": "MinMaxIA",
    "NNItemAssignment": "NNIA",
    "SinglePosItemAssignment": "SinglePosIA",
    "FifoBatching": "FiFo",
    "FiFo": "FiFo",
    "OrderNrFifoBatching": "OrdNr",
    "OrderNrFiFo": "OrdNr",
    "DueDateBatching": "DueDate",
    "DueDate": "DueDate",
    "RandomBatching": "RAND",
    "Random": "RAND",
    "SingleOrderBatching": "SingleOrderBatching",
    "ClarkAndWrightNN": "SavingsNN",
    "ClarkAndWrightRR": "SavingsRR",
    "ClarkAndWrightSShape": "SavingsSShape",
    "ClosestDepotMinDistanceSeedBatching": "SEEDCDMinDist",
    "ClosestDepotMaxSharedArticlesSeedBatching": "SEEDCDMaxArticles",
    "LSBatchingNNFiFo": "LSFiFoNN",
    "LSBatchingNNFiFoOrderNr": "LSOrdNrNN",
    "LSBatchingNNDueDate": "LSDueDateNN",
    "LSBatchingRRFiFo": "LSBatchingRRFiFo",
    "LSBatchingRROrderNrFiFo": "LSBatchingRROrderNrFiFo",
    "LSBatchingRRDueDate": "LSBatchingRRDueDate",
    "LSBatchingSShapeFiFo": "LSBatchingSShapeFiFo",
    "LSBatchingSShapeFiFoOrderNr": "LSBatchingSShapeFiFoOrderNr",
    "LSBatchingSShapeDueDate": "LSBatchingSShapeDueDate",
    "SShapeRouting": "SShape",
    "SShape": "SShape",
    "MidpointRouting": "MP",
    "Midpoint": "MP",
    "LargestGapRouting": "LG",
    "LargestGap": "LG",
    "ReturnRouting": "RET",
    "Return": "RET",
    "NearestNeighbourhoodRouting": "NN",
    "NearestNeighbourhood": "NN",
    "RatliffRosenthalRouting": "RR",
    "RatliffRosenthal": "RR",
    # NOTE: make_df_results.py does NOT map RatliffRosenthalNF, so the df
    # keeps the full class name.  We match the df's actual convention here.
    "RatliffRosenthalNF": "RatliffRosenthalNF",
    "ExactTSPRoutingDistance": "TSP",
    "ExactSolving": "TSP",
    "CombinedBatchingRoutingAssigning": "CombinedBatchingRoutingAssigning",
    "SPTScheduling": "SPT",
    "SPTScheduler": "SPT",
    "LPTScheduling": "LPT",
    "LPTScheduler": "LPT",
    "EDDScheduling": "EDD",
    "EDDScheduler": "EDD",
}

# Legacy class names that have NO current configured equivalent.
# These unambiguously identify legacy results.
LEGACY_ONLY_NAMES = {
    "LSBatchingRR",        # legacy single impl; current = LSBatchingRRFiFo / RROrderNrFiFo / RRDueDate
    "LSBatchingNNRand",    # legacy; no current configured equivalent
    "LSRANDNN",            # short name for LSBatchingNNRand
    "LSRANDRR",            # short name for legacy RR+Random LS
    "LSFiFoRR",            # short name for legacy RR+FiFo LS
}

# Deliberately excluded from the experiment scope (not missing — a design
# choice, not a coverage gap).
EXCLUDED_FROM_EXPERIMENTS = {
    "ExactSolving",        # TSP exact routing — computationally infeasible at scale
    "TSP",
}

# RR-NF implementation requires scattered storage.  The card declares
# storage: {type: [any]}, which over-admits it to dedicated-storage sets.
# Only SPRP-SS has scattered storage among the benchmark sets.
SCATTERED_STORAGE_SETS = {"SPRP-SS"}

# CBR (CombinedBatchingRoutingAssigning) is deliberately excluded from the
# experiment scope on all OBRP sets except BahceciOencan.
CBR_EXCLUDED_SETS = {"FoodmartData", "HennWaescherUniform", "HennWaescherClassBased", "MuterOencan"}

# Configured component names that exist in BOTH legacy modules and current
# generated modules (same class name, different module path).  We cannot
# distinguish them from the df alone.
AMBIGUOUS_CONFIGURED_NAMES = {
    "ClarkAndWrightNN", "ClarkAndWrightRR", "ClarkAndWrightSShape",
    "ClosestDepotMinDistanceSeedBatching",
    "ClosestDepotMaxSharedArticlesSeedBatching",
    "LSBatchingNNFiFo", "LSBatchingNNFiFoOrderNr", "LSBatchingNNDueDate",
    "LSBatchingSShapeFiFoOrderNr",
    # short-name forms
    "SavingsNN", "SavingsRR", "SavingsSShape",
    "SEEDCDMinDist", "SEEDCDMaxArticles",
    "LSFiFoNN", "LSOrdNrNN", "LSDueDateNN",
}

# Configured component names that are NEW (only in generated modules, no
# legacy class with the same name).  Their presence in the df unambiguously
# confirms current-path execution.
NEW_CONFIGURED_NAMES = {
    "LSBatchingRRFiFo", "LSBatchingRROrderNrFiFo", "LSBatchingRRDueDate",
    "LSBatchingSShapeFiFo", "LSBatchingSShapeDueDate",
}


def short_name(card_name: str) -> str:
    return SHORT_NAMES.get(card_name, card_name)


def problem_class_of(data_card_path: Path) -> str:
    dc = load_and_flatten_data_card(data_card_path)
    return dc.problem_class


def applicable_cards_for(data_card_path: Path, all_cards, mapper):
    dc = load_and_flatten_data_card(data_card_path)
    return mapper.filter(algorithms=all_cards, instance=dc, verbose=False)


def classify_batching_name(name: str) -> str:
    """Classify a batching_algo value from the df."""
    if name in LEGACY_ONLY_NAMES:
        return "legacy_implementation"
    if name in NEW_CONFIGURED_NAMES:
        return "current_config_result"
    if name in AMBIGUOUS_CONFIGURED_NAMES:
        return "ambiguous_configured"
    # Base cards that are the same in legacy and current (module never changed)
    BASE_CURRENT = {"DueDate", "FiFo", "OrdNr", "RAND", "SingleOrderBatching"}
    if name in BASE_CURRENT:
        return "current_config_result"
    return "unresolved"


def build_manifest() -> pd.DataFrame:
    """Build the current portfolio manifest from cards + configs + data cards."""
    all_cards = load_packaged_algo_cards()

    # Load configuration names from configs.yaml to distinguish configured
    # cards from base cards.
    from ware_ops_algos.algorithms.algorithm_cards import load_configurations
    cards_resource_dir = Path(
        __import__("ware_ops_algos").__file__
    ).parent / "algorithms" / "algorithm_cards"
    configs = load_configurations(cards_resource_dir)
    configured_names = {cfg["name"] for cfg in configs}

    mapper = DomainAlgorithmMapper(TAXONOMY)

    rows: list[dict[str, Any]] = []
    for card_file in sorted(CARDS_DIR.glob("*.yaml")):
        stem = card_file.stem
        isets = DATA_CARD_TO_INSTANCE_SETS.get(stem, [stem])
        applicable = applicable_cards_for(card_file, all_cards, mapper)
        dc = load_and_flatten_data_card(card_file)
        for c in applicable:
            for iset in isets:
                rows.append({
                    "instance_set": iset,
                    "instance_set_display": INSTANCE_SET_DISPLAY.get(iset, iset),
                    "problem_class": dc.problem_class,
                    "algo_name": c.algo_name,
                    "df_name": short_name(c.algo_name),
                    "problem_type": c.problem_type,
                    "impl_class": c.implementation.get("class_name", ""),
                    "component_name": c.implementation.get("component_name", ""),
                    "has_configuration": bool(c.configuration),
                    "is_configured": c.algo_name in configured_names,
                    "objective": c.objective or "",
                })
    return pd.DataFrame(rows)


def enumerate_expected_pipelines(manifest: pd.DataFrame) -> dict[str, list[str]]:
    """Enumerate expected pipeline strategies per instance set.

    Pipeline structure:
      SPRP:     IA × SingleOrderBatching × Routing   (sequential)
                + RR-NF as IAR (CombinedIAR, no batching)
      SPRP-SS:  same as SPRP but with multiple IA
      OBRP:     IA × Batching × Routing               (sequential)
                + CBR (CombinedBR, no separate routing)
      OBSRP:    IA × Batching × Routing                (distance)
                + IA × Batching × Routing × Scheduling  (due-date)
    """
    by_set: dict[str, list[str]] = {}

    for iset, group in manifest.groupby("instance_set"):
        pclass = group["problem_class"].iloc[0]
        ia = sorted(group[group["problem_type"] == "item_assignment"]["df_name"].unique())
        batching = sorted(group[group["problem_type"] == "batching"]["df_name"].unique())
        routing = sorted(group[group["problem_type"] == "routing"]["df_name"].unique())
        scheduling = sorted(group[group["problem_type"] == "scheduling"]["df_name"].unique())
        br = sorted(group[group["problem_type"] == "batching_routing"]["df_name"].unique())

        # Ensure SingleOrderBatching is always present (it's a template, not a card)
        if "SingleOrderBatching" not in batching:
            batching = sorted(set(batching) | {"SingleOrderBatching"})

        # Exclude deliberately-excluded algos (TSP exact routing — not run
        # by design, not a coverage gap).
        routing = [r for r in routing if r not in EXCLUDED_FROM_EXPERIMENTS]

        # RR-NF requires scattered storage; only SPRP-SS has it.
        # The card over-admits it (storage: any), so filter here.
        if iset not in SCATTERED_STORAGE_SETS:
            routing = [r for r in routing if r != "RatliffRosenthalNF"]

        # CBR is deliberately excluded on all OBRP sets except BahceciOencan.
        if iset in CBR_EXCLUDED_SETS:
            br = []

        pipelines: list[str] = []

        if pclass == "SPRP":
            # Sequential: IA × SingleOrderBatching × Routing
            for a in ia:
                for r in routing:
                    if r == "RatliffRosenthalNF":
                        continue  # RR-NF is IAR, not sequential
                    pipelines.append(f"{a}+SingleOrderBatching+{r}")
            # IAR: RR-NF (only on scattered storage, i.e. SPRP-SS)
            if "RatliffRosenthalNF" in routing:
                pipelines.append("RatliffRosenthalNF")

        elif pclass == "OBRP":
            # Sequential: IA × Batching × Routing (exclude CBR from routing)
            for a in ia:
                for b in batching:
                    for r in routing:
                        if r == "CombinedBatchingRoutingAssigning":
                            continue
                        pipelines.append(f"{a}+{b}+{r}")
            # CBR: IA × CBR (no separate routing)
            for a in ia:
                for c in br:
                    pipelines.append(f"{a}+{c}")
            # No IAR on OBRP (dedicated storage)

        elif pclass == "OBSRP":
            # Distance pipelines: IA × Batching × Routing
            for a in ia:
                for b in batching:
                    for r in routing:
                        if r == "CombinedBatchingRoutingAssigning":
                            continue
                        pipelines.append(f"{a}+{b}+{r}")
            # Due-date pipelines: IA × Batching × Routing × Scheduling
            for a in ia:
                for b in batching:
                    for r in routing:
                        if r == "CombinedBatchingRoutingAssigning":
                            continue
                        for s in scheduling:
                            pipelines.append(f"{a}+{b}+{r}+{s}")

        by_set[iset] = sorted(set(pipelines))

    return by_set


def main() -> None:
    print("Loading algorithm cards ...")
    manifest = build_manifest()
    manifest_path = GENERATED_DIR / "current_portfolio_manifest.csv"
    manifest.to_csv(manifest_path, index=False)
    print(f"Wrote {manifest_path} ({len(manifest)} rows)")

    print("\nEnumerating expected pipelines ...")
    expected = enumerate_expected_pipelines(manifest)
    for iset, pipes in sorted(expected.items()):
        print(f"  {iset}: {len(pipes)} expected pipelines")

    print("\nLoading df_results.pkl ...")
    df = pd.read_pickle(DF_PATH)

    # Apply SHORT_NAMES to match df (the pickle already has short names applied
    # by make_df_results.py, so the df values are already short names).

    print("\nClassifying result rows ...")
    audit_rows: list[dict[str, Any]] = []

    # For each instance set, determine if any legacy-only names are present.
    # If so, the entire set's ambiguous results are classified as legacy
    # (the run predates the current generated modules).
    set_has_legacy = {}
    set_has_new = {}
    for iset in df["instance_set"].unique():
        sub = df[df["instance_set"] == iset]
        all_batching = set(sub["batching_algo"].dropna().astype(str))
        all_batching = {v for v in all_batching if v and v != "nan"}
        has_legacy = bool(all_batching & LEGACY_ONLY_NAMES)
        has_new = bool(all_batching & NEW_CONFIGURED_NAMES)
        set_has_legacy[iset] = has_legacy
        set_has_new[iset] = has_new

    # Build expected strategy sets per instance set (merging sub-sets)
    expected_by_display: dict[str, set[str]] = {}
    for iset, pipes in expected.items():
        display = INSTANCE_SET_DISPLAY.get(iset, iset)
        if display not in expected_by_display:
            expected_by_display[display] = set()
        expected_by_display[display] |= set(pipes)

    # Classify each unique strategy in the df
    for iset in sorted(df["instance_set"].unique()):
        sub = df[df["instance_set"] == iset]
        display = INSTANCE_SET_DISPLAY.get(iset, iset)
        expected_strats = expected_by_display.get(display, set())

        for strat in sorted(sub["strategy"].unique()):
            n_rows = (sub["strategy"] == strat).sum()
            parts = strat.split("+")
            batching_name = parts[1] if len(parts) > 1 else ""

            # Classify
            has_legacy_part = any(p in LEGACY_ONLY_NAMES for p in parts)
            has_ambiguous = any(p in AMBIGUOUS_CONFIGURED_NAMES for p in parts)

            if has_legacy_part:
                classification = "legacy_implementation"
                reason = f"Contains legacy-only name: {[p for p in parts if p in LEGACY_ONLY_NAMES]}"
            elif set_has_legacy[iset] and has_ambiguous:
                classification = "legacy_implementation"
                reason = "Ambiguous name in a set confirmed legacy by co-occurring legacy-only name"
            elif strat in expected_strats:
                classification = "current_config_result"
                reason = "Matches a current configured pipeline"
            else:
                classification = "unresolved"
                reason = "Does not match any expected current pipeline"

            audit_rows.append({
                "instance_set": iset,
                "instance_set_display": display,
                "strategy": strat,
                "n_rows": int(n_rows),
                "classification": classification,
                "reason": reason,
                "set_has_legacy": set_has_legacy[iset],
            })

    audit_df = pd.DataFrame(audit_rows)
    audit_path = GENERATED_DIR / "result_configuration_audit.csv"
    audit_df.to_csv(audit_path, index=False)
    print(f"Wrote {audit_path} ({len(audit_df)} rows)")

    print("\nBuilding missing-results matrix ...")
    missing_rows: list[dict[str, Any]] = []

    for iset, pipes in sorted(expected.items()):
        display = INSTANCE_SET_DISPLAY.get(iset, iset)
        # Get all df strategies for this instance set (or its sub-sets)
        df_strats: set[str] = set()
        for df_iset in df["instance_set"].unique():
            if INSTANCE_SET_DISPLAY.get(df_iset, df_iset) == display:
                df_strats |= set(df[df["instance_set"] == df_iset]["strategy"].unique())

        # Count expected instances
        n_expected = df[df["instance_set"] == iset]["instance_name"].nunique()

        for pipe in sorted(pipes):
            if pipe not in df_strats:
                # Determine what's missing
                parts = pipe.split("+")
                missing_component = ""
                if len(parts) == 1 and parts[0] == "RatliffRosenthalNF":
                    missing_component = "RR-NF (IAR pipeline)"
                elif len(parts) == 2 and "CombinedBatchingRoutingAssigning" in parts:
                    missing_component = "CBR pipeline"
                else:
                    # Check which component is new vs legacy
                    for p in parts:
                        raw_name = p
                        if p in NEW_CONFIGURED_NAMES:
                            missing_component = f"new configured: {p}"
                            break
                        if p in AMBIGUOUS_CONFIGURED_NAMES and not set_has_legacy.get(iset, False):
                            # Could be a genuinely missing current config
                            missing_component = f"configured: {p}"
                            break
                    if not missing_component:
                        missing_component = "full pipeline"

                missing_rows.append({
                    "instance_set": iset,
                    "instance_set_display": display,
                    "pipeline": pipe,
                    "expected_instances": int(n_expected),
                    "valid_results": 0,
                    "missing_count": int(n_expected),
                    "missing_component": missing_component,
                    "rerun_required": "yes",
                })

    missing_df = pd.DataFrame(missing_rows)
    missing_path = GENERATED_DIR / "missing_current_results.csv"
    missing_df.to_csv(missing_path, index=False)
    print(f"Wrote {missing_path} ({len(missing_df)} missing pipelines)")

    # Summary
    print("\n" + "=" * 70)
    print("AUDIT SUMMARY")
    print("=" * 70)
    for iset in sorted(df["instance_set"].unique()):
        display = INSTANCE_SET_DISPLAY.get(iset, iset)
        sub_audit = audit_df[audit_df["instance_set"] == iset]
        n_current = (sub_audit["classification"] == "current_config_result").sum()
        n_legacy = (sub_audit["classification"] == "legacy_implementation").sum()
        n_unresolved = (sub_audit["classification"] == "unresolved").sum()
        n_missing = len(missing_df[missing_df["instance_set"] == iset]) if len(missing_df) > 0 else 0
        print(f"  {iset:30s} current={n_current:3d}  legacy={n_legacy:3d}  unresolved={n_unresolved:3d}  missing={n_missing:3d}")

    total_missing = len(missing_df)
    total_legacy = (audit_df["classification"] == "legacy_implementation").sum()
    print(f"\n  TOTAL: {total_legacy} legacy strategies, {total_missing} missing pipelines")

    if total_missing > 0 or total_legacy > 0:
        print("\n  >>> RERUN REQUIRED <<<")
    else:
        print("\n  >>> CONFIG-ONLY PORTFOLIO COMPLETE <<<")


if __name__ == "__main__":
    main()
