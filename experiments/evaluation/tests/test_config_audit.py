"""
Focused tests for the configuration-driven portfolio audit.

Tests use synthetic dataframes and the current algorithm-card system.
No warehouse algorithms are executed.
"""

from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


# ---------------------------------------------------------------------------
# Test helpers
# ---------------------------------------------------------------------------

def _make_df(rows: list[dict]) -> pd.DataFrame:
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# 1. Legacy-only name detection
# ---------------------------------------------------------------------------

def test_legacy_only_names_excluded() -> None:
    """LSBatchingRR and LSBatchingNNRand are legacy-only names."""
    from config_audit import LEGACY_ONLY_NAMES
    assert "LSBatchingRR" in LEGACY_ONLY_NAMES
    assert "LSBatchingNNRand" in LEGACY_ONLY_NAMES
    assert "LSRANDNN" in LEGACY_ONLY_NAMES
    # Current configured names must NOT be in legacy-only set
    assert "LSBatchingRRFiFo" not in LEGACY_ONLY_NAMES
    assert "LSBatchingRROrderNrFiFo" not in LEGACY_ONLY_NAMES
    assert "LSBatchingSShapeFiFoOrderNr" not in LEGACY_ONLY_NAMES


def test_ambiguous_names_identified() -> None:
    """Names that exist in both legacy and current modules are ambiguous."""
    from config_audit import AMBIGUOUS_CONFIGURED_NAMES
    assert "ClarkAndWrightNN" in AMBIGUOUS_CONFIGURED_NAMES
    assert "SavingsNN" in AMBIGUOUS_CONFIGURED_NAMES
    assert "LSOrdNrNN" in AMBIGUOUS_CONFIGURED_NAMES
    # New-only names must NOT be ambiguous
    from config_audit import NEW_CONFIGURED_NAMES
    assert "LSBatchingRRFiFo" in NEW_CONFIGURED_NAMES
    assert "LSBatchingSShapeDueDate" in NEW_CONFIGURED_NAMES
    assert NEW_CONFIGURED_NAMES.isdisjoint(AMBIGUOUS_CONFIGURED_NAMES)


# ---------------------------------------------------------------------------
# 2. Refusal to alias legacy and current implementations
# ---------------------------------------------------------------------------

def test_no_alias_legacy_to_current() -> None:
    """The audit must not map LSBatchingRR to LSBatchingRROrderNrFiFo."""
    from config_audit import SHORT_NAMES
    # LSBatchingRR is NOT mapped to any current configured name
    assert SHORT_NAMES.get("LSBatchingRR", "LSBatchingRR") == "LSBatchingRR"
    assert SHORT_NAMES.get("LSBatchingNNRand", "LSBatchingNNRand") == "LSBatchingNNRand"
    # LSBatchingRROrderNrFiFo keeps its own identity
    assert SHORT_NAMES.get("LSBatchingRROrderNrFiFo") == "LSBatchingRROrderNrFiFo"


# ---------------------------------------------------------------------------
# 3. Fail-loudly guard in generate_paper_outputs
# ---------------------------------------------------------------------------

def test_fail_if_legacy_results_raises() -> None:
    """The fail-loudly guard must reject dataframes with legacy names."""
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    from generate_paper_outputs import fail_if_legacy_results
    df = _make_df([
        {"batching_algo": "LSBatchingRR", "routing_algo": "RR",
         "item_assignment_algo": "GIA", "scheduling_algo": None},
    ])
    try:
        fail_if_legacy_results(df)
        raise AssertionError("Should have raised RuntimeError")
    except RuntimeError as exc:
        assert "LSBatchingRR" in str(exc)


def test_fail_if_legacy_results_passes_clean() -> None:
    """The guard must NOT raise for a clean current-only dataframe."""
    from generate_paper_outputs import fail_if_legacy_results
    df = _make_df([
        {"batching_algo": "FiFo", "routing_algo": "RR",
         "item_assignment_algo": "GIA", "scheduling_algo": None},
        {"batching_algo": "LSBatchingRROrderNrFiFo", "routing_algo": "RR",
         "item_assignment_algo": "GIA", "scheduling_algo": None},
    ])
    fail_if_legacy_results(df)  # must not raise


def test_fail_if_legacy_detects_lsvandnn() -> None:
    """The guard must detect LSRANDNN (legacy LSBatchingNNRand)."""
    from generate_paper_outputs import fail_if_legacy_results
    df = _make_df([
        {"batching_algo": "LSRANDNN", "routing_algo": "NN",
         "item_assignment_algo": "GIA", "scheduling_algo": None},
    ])
    try:
        fail_if_legacy_results(df)
        raise AssertionError("Should have raised RuntimeError")
    except RuntimeError as exc:
        assert "LSRANDNN" in str(exc)


# ---------------------------------------------------------------------------
# 4. Manifest generation
# ---------------------------------------------------------------------------

def test_manifest_has_expected_instance_sets() -> None:
    """The manifest must cover all 7 data cards / 9 df instance sets."""
    from config_audit import build_manifest
    manifest = build_manifest()
    isets = set(manifest["instance_set"])
    expected = {
        "SPRP", "SPRP-SS", "BahceciOencan",
        "HennWaescherUniform", "HennWaescherClassBased",
        "MuterOencan", "FoodmartData",
        "KrisSmallDataCorrected", "KrisLargeData",
    }
    assert isets == expected, f"Missing: {expected - isets}, Extra: {isets - expected}"


def test_manifest_rrnf_applicable_sets() -> None:
    """RR-NF card now correctly restricts to scattered storage.
    After the card fix, RR-NF is admitted only for SPRP-SS."""
    from config_audit import build_manifest
    manifest = build_manifest()
    rrnf = manifest[manifest["algo_name"] == "RatliffRosenthalNF"]
    rrnf_sets = set(rrnf["instance_set"])
    # Card now restricts to scattered storage — only SPRP-SS qualifies
    assert "SPRP-SS" in rrnf_sets
    assert "SPRP" not in rrnf_sets
    assert "BahceciOencan" not in rrnf_sets
    assert "HennWaescherUniform" not in rrnf_sets
    assert "MuterOencan" not in rrnf_sets
    assert "KrisSmallDataCorrected" not in rrnf_sets


def test_manifest_configured_components_present() -> None:
    """All applicable configured components must appear in the manifest."""
    from config_audit import build_manifest
    manifest = build_manifest()
    configured = manifest[manifest["is_configured"] == True]
    names = set(configured["algo_name"])
    # 12 of the 14 configured components are applicable to at least one set.
    # LSBatchingRRFiFo and LSBatchingRRDueDate are NOT applicable to any
    # current benchmark: RR requires n_blocks=1, but the n_blocks=1 sets
    # lack the order_date/due_date features needed by FiFo/DueDate starts.
    expected = {
        "LSBatchingNNFiFo", "LSBatchingNNFiFoOrderNr", "LSBatchingNNDueDate",
        "LSBatchingRROrderNrFiFo",
        "LSBatchingSShapeFiFo", "LSBatchingSShapeFiFoOrderNr", "LSBatchingSShapeDueDate",
        "ClarkAndWrightNN", "ClarkAndWrightRR", "ClarkAndWrightSShape",
        "ClosestDepotMinDistanceSeedBatching", "ClosestDepotMaxSharedArticlesSeedBatching",
    }
    assert names == expected, f"Missing: {expected - names}, Extra: {names - expected}"


# ---------------------------------------------------------------------------
# 5. Pipeline enumeration
# ---------------------------------------------------------------------------

def test_tsp_excluded_from_experiments() -> None:
    """TSP (ExactSolving) is deliberately excluded from the experiment scope."""
    from config_audit import EXCLUDED_FROM_EXPERIMENTS
    assert "ExactSolving" in EXCLUDED_FROM_EXPERIMENTS
    assert "TSP" in EXCLUDED_FROM_EXPERIMENTS
    # No expected pipeline should contain TSP
    from config_audit import build_manifest, enumerate_expected_pipelines
    manifest = build_manifest()
    expected = enumerate_expected_pipelines(manifest)
    for iset, pipes in expected.items():
        for p in pipes:
            assert "TSP" not in p, f"TSP found in {iset}: {p}"


def test_rrnf_only_on_scattered_storage() -> None:
    """RR-NF must only appear as an expected pipeline on SPRP-SS (scattered)."""
    from config_audit import build_manifest, enumerate_expected_pipelines, SCATTERED_STORAGE_SETS
    manifest = build_manifest()
    expected = enumerate_expected_pipelines(manifest)
    for iset, pipes in expected.items():
        has_rrnf = any("RatliffRosenthalNF" == p for p in pipes)
        if iset in SCATTERED_STORAGE_SETS:
            assert has_rrnf, f"RR-NF should be on {iset}"
        else:
            assert not has_rrnf, f"RR-NF should NOT be on {iset} (not scattered)"


def test_cbr_only_on_bahceci() -> None:
    """CBR is deliberately excluded on all OBRP sets except BahceciOencan."""
    from config_audit import build_manifest, enumerate_expected_pipelines, CBR_EXCLUDED_SETS
    manifest = build_manifest()
    expected = enumerate_expected_pipelines(manifest)
    for iset, pipes in expected.items():
        has_cbr = any("CombinedBatchingRoutingAssigning" in p for p in pipes)
        if iset == "BahceciOencan":
            assert has_cbr, f"CBR should be on {iset}"
        elif iset in CBR_EXCLUDED_SETS:
            assert not has_cbr, f"CBR should NOT be on {iset} (excluded by design)"


def test_sprp_expected_pipelines() -> None:
    """SPRP should have 6 expected pipelines (1 IA × 6 routing, no TSP, no RR-NF)."""
    from config_audit import build_manifest, enumerate_expected_pipelines
    manifest = build_manifest()
    expected = enumerate_expected_pipelines(manifest)
    sprp = expected["SPRP"]
    # 1 IA × 6 routing (LG, MP, NN, RET, RR, SShape) — TSP excluded, RR-NF excluded (dedicated storage)
    assert len(sprp) == 6
    assert "GIA+SingleOrderBatching+RR" in sprp
    assert "RatliffRosenthalNF" not in sprp  # No IAR on dedicated storage
    assert "GIA+SingleOrderBatching+TSP" not in sprp  # TSP excluded by design


def test_sprpss_expected_pipelines() -> None:
    """SPRP-SS should have 31 expected pipelines (5 IA × 6 routing + 1 RR-NF)."""
    from config_audit import build_manifest, enumerate_expected_pipelines
    manifest = build_manifest()
    expected = enumerate_expected_pipelines(manifest)
    sprpss = expected["SPRP-SS"]
    # 5 IA × 6 routing = 30 sequential + 1 IAR (RR-NF) = 31
    assert len(sprpss) == 31
    assert "RatliffRosenthalNF" in sprpss  # IAR on scattered storage


def test_kris_expected_has_scheduling() -> None:
    """Kris pipelines must include scheduling variants."""
    from config_audit import build_manifest, enumerate_expected_pipelines
    manifest = build_manifest()
    expected = enumerate_expected_pipelines(manifest)
    kris = expected["KrisSmallDataCorrected"]
    # Some pipelines must have scheduling
    sched_pipes = [p for p in kris if "+SPT" in p or "+LPT" in p or "+EDD" in p]
    assert len(sched_pipes) > 0
    # Some must be distance-only (no scheduling)
    dist_pipes = [p for p in kris if "+SPT" not in p and "+LPT" not in p and "+EDD" not in p]
    assert len(dist_pipes) > 0


# ---------------------------------------------------------------------------
# 6. VBS and SBS on synthetic data (exact equality for integer objectives)
# ---------------------------------------------------------------------------

def test_vbs_exact_equality_integer() -> None:
    """VBS must use exact equality for integer-valued objectives."""
    df = _make_df([
        {"instance_name": "i1", "strategy": "A", "total_distance": 100},
        {"instance_name": "i1", "strategy": "B", "total_distance": 100},
        {"instance_name": "i1", "strategy": "C", "total_distance": 101},
        {"instance_name": "i2", "strategy": "A", "total_distance": 200},
        {"instance_name": "i2", "strategy": "B", "total_distance": 201},
        {"instance_name": "i2", "strategy": "C", "total_distance": 202},
    ])
    # VBS per instance (min)
    vbs = df.groupby("instance_name")["total_distance"].min()
    assert vbs["i1"] == 100
    assert vbs["i2"] == 200
    # Both A and B attain VBS on i1 (exact tie)
    i1_best = df[(df["instance_name"] == "i1") & (df["total_distance"] == 100)]
    assert set(i1_best["strategy"]) == {"A", "B"}


def test_sbs_attainment() -> None:
    """SBS attainment = fraction of instances where SBS is in VBS set."""
    df = _make_df([
        {"instance_name": "i1", "strategy": "A", "total_distance": 100},
        {"instance_name": "i1", "strategy": "B", "total_distance": 100},
        {"instance_name": "i2", "strategy": "A", "total_distance": 200},
        {"instance_name": "i2", "strategy": "B", "total_distance": 201},
    ])
    means = df.groupby("strategy")["total_distance"].mean()
    sbs = means.idxmin()  # A (150) < B (150.5)
    assert sbs == "A"
    # SBS = A attains VBS on both instances
    for inst in ["i1", "i2"]:
        vbs = df[df["instance_name"] == inst]["total_distance"].min()
        sbs_val = df[(df["instance_name"] == inst) & (df["strategy"] == sbs)]["total_distance"].iloc[0]
        assert sbs_val == vbs, f"SBS does not attain VBS on {inst}"


# ---------------------------------------------------------------------------
# 7. Winner credit with distinct pipelines
# ---------------------------------------------------------------------------

def test_winner_credit_distinct_pipelines() -> None:
    """Fractional winner credit must use distinct pipelines, not duplicate rows."""
    df = _make_df([
        {"instance_name": "i1", "strategy": "A", "total_distance": 100},
        {"instance_name": "i1", "strategy": "B", "total_distance": 100},
        {"instance_name": "i2", "strategy": "A", "total_distance": 200},
        {"instance_name": "i2", "strategy": "B", "total_distance": 200},
    ])
    # VBS ties: both A and B tie on both instances
    # Winner credit: A gets 0.5 + 0.5 = 1.0, B gets 0.5 + 0.5 = 1.0
    total_credit = {}
    for inst in ["i1", "i2"]:
        vbs = df[df["instance_name"] == inst]["total_distance"].min()
        winners = df[(df["instance_name"] == inst) & (df["total_distance"] == vbs)]
        credit = 1.0 / len(winners)
        for _, w in winners.iterrows():
            total_credit[w["strategy"]] = total_credit.get(w["strategy"], 0) + credit
    # Shares sum to 100%
    assert abs(sum(total_credit.values()) - 2.0) < 1e-9
    # Each gets 50% of total credit
    assert abs(total_credit["A"] - 1.0) < 1e-9
    assert abs(total_credit["B"] - 1.0) < 1e-9


# ---------------------------------------------------------------------------
# 8. Component classification
# ---------------------------------------------------------------------------

def test_component_classification_sequential() -> None:
    """A regular pipeline GIA+FiFo+RR is sequential (not IAR/BR)."""
    from data import IAR_ALGOS, BR_ALGOS
    assert "RatliffRosenthalNF" in IAR_ALGOS or "RR-NF" in IAR_ALGOS
    assert "CombinedBatchingRoutingAssigning" in BR_ALGOS or "CBR" in BR_ALGOS
    # FiFo and RR are NOT IAR/BR
    assert "FiFo" not in IAR_ALGOS
    assert "RR" not in IAR_ALGOS
    assert "FiFo" not in BR_ALGOS


def test_atomic_treatment_rrnf() -> None:
    """RR-NF is an IAR component, treated atomically (no separate IA/batching)."""
    from data import IAR_ALGOS
    # RR-NF should be in IAR_ALGOS
    assert "RatliffRosenthalNF" in IAR_ALGOS or "RR-NF" in IAR_ALGOS
    # RR-NF pipeline has no separate IA or batching
    # (verified by the IAR pipeline structure in enumerate_expected_pipelines)


def test_atomic_treatment_cbr() -> None:
    """CBR is a BR component, treated atomically (no separate batching/routing)."""
    from data import BR_ALGOS
    assert "CombinedBatchingRoutingAssigning" in BR_ALGOS or "CBR" in BR_ALGOS
