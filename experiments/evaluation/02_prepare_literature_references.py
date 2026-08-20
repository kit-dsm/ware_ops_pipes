"""Prepare the Wahlen--Gschwind reference results."""

from __future__ import annotations

import re
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[2]
REFERENCE_DIR = ROOT / "data" / "reference"
WG_RAW = REFERENCE_DIR / "raw" / "wahlen_gschwind_2023"
HI_RAW = REFERENCE_DIR / "raw" / "hessler_irnich"
TRANSCRIBED = REFERENCE_DIR / "transcribed"
PROCESSED = REFERENCE_DIR / "processed"

HENN_IDENTIFIERS = HI_RAW / "results_HennWaescher.csv"
FOODMART_B15 = TRANSCRIBED / "wahlen_2026_foodmart_table_b15.csv"

POLICIES = [
    "traversal",
    "return",
    "midpoint",
    "largest_gap",
    "combined",
    "optimal",
]


def parse_wg_table(path: Path) -> pd.DataFrame:
    """Read a Wahlen--Gschwind result file."""
    raw = pd.read_csv(path, sep=";", header=None, skiprows=3)
    raw = raw.dropna(axis=1, how="all")

    out = raw.iloc[:, :3].copy()
    out.columns = ["capacity", "num_orders", "inst_num"]
    for column in out.columns:
        out[column] = pd.to_numeric(out[column], errors="raise").astype(int)

    start = 3
    for policy in POLICIES:
        block = raw.iloc[:, start : start + 8]
        out[f"{policy}_bks"] = pd.to_numeric(block.iloc[:, 0], errors="coerce")
        out[f"{policy}_bks_is_opt"] = (
            block.iloc[:, 1]
            .astype(str)
            .str.strip()
            .str.lower()
            .eq("true")
        )
        start += 8
    return out


def build_henn_reference() -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    pattern = re.compile(
        r"detailedResults-HW_(CBD|UDD)_(LargestGap|Traversal)\.csv"
    )

    for path in sorted(WG_RAW.glob("detailedResults-HW_*.csv")):
        match = pattern.fullmatch(path.name)
        if not match:
            continue
        storage, source_variant = match.groups()
        frame = parse_wg_table(path)
        frame.insert(0, "storage", storage)
        frame.insert(1, "source_variant", source_variant)
        frames.append(frame)

    if len(frames) != 4:
        raise ValueError(f"Expected four Henn/Waescher source files, found {len(frames)}")

    out = pd.concat(frames, ignore_index=True)
    if len(out) != 5760:
        raise ValueError(f"Expected 5,760 Henn/Waescher rows, found {len(out)}")

    value_columns = [f"{policy}_bks" for policy in POLICIES]
    out["bks_value"] = out[value_columns].min(axis=1)
    out["bks_source_policy"] = (
        out[value_columns].idxmin(axis=1).str.removesuffix("_bks")
    )
    out["bks_is_opt"] = out.apply(
        lambda row: bool(row[f"{row['bks_source_policy']}_bks_is_opt"]),
        axis=1,
    )
    out["optimal_block_bks"] = out["optimal_bks"]
    out["any_policy_improves_optimal"] = (
        out["bks_value"] < out["optimal_block_bks"] - 1e-9
    )

    if int(out["bks_is_opt"].sum()) != 5458:
        raise ValueError("Unexpected Henn/Waescher optimum count")
    if int(out["any_policy_improves_optimal"].sum()) != 1:
        raise ValueError("Expected exactly one Henn row improving the optimal block")

    out["source"] = "Wahlen & Gschwind (2023)"
    out["doi"] = "10.1287/trsc.2023.1198"
    return out[
        [
            "storage",
            "source_variant",
            "capacity",
            "num_orders",
            "inst_num",
            "bks_value",
            "bks_is_opt",
            "bks_source_policy",
            "optimal_block_bks",
            "any_policy_improves_optimal",
            "source",
            "doi",
        ]
    ]


def build_muter_wg_reference() -> pd.DataFrame:
    out = parse_wg_table(WG_RAW / "detailedResults-MO.csv")
    if len(out) != 270:
        raise ValueError(f"Expected 270 Muter/Oencan rows, found {len(out)}")

    value_columns = [f"{policy}_bks" for policy in POLICIES]
    out["best_any_policy_bks"] = out[value_columns].min(axis=1)
    out["best_any_policy"] = (
        out[value_columns].idxmin(axis=1).str.removesuffix("_bks")
    )
    out["optimal_bks_is_opt"] = out["optimal_bks_is_opt"].astype(bool)
    out["any_policy_improves_optimal"] = (
        out["best_any_policy_bks"] < out["optimal_bks"] - 1e-9
    )
    out["directly_comparable_to_current_casop"] = False
    out["comparability_note"] = (
        "Use only with MuterOencanWG. Wahlen--Gschwind assume 1.5-unit "
        "aisle/depot entry distances; the original CASOP geometry uses 1.0."
    )
    out["source"] = "Wahlen & Gschwind (2023)"
    out["doi"] = "10.1287/trsc.2023.1198"

    if int(out["optimal_bks_is_opt"].sum()) != 234:
        raise ValueError("Unexpected Muter/Oencan-WG optimum count")

    return out[
        [
            "capacity",
            "num_orders",
            "inst_num",
            "optimal_bks",
            "optimal_bks_is_opt",
            "best_any_policy_bks",
            "best_any_policy",
            "any_policy_improves_optimal",
            "directly_comparable_to_current_casop",
            "comparability_note",
            "source",
            "doi",
        ]
    ]


def validate_foodmart_transcription() -> None:
    foodmart = pd.read_csv(FOODMART_B15)
    required = {
        "delta",
        "orders_n",
        "aisles_S",
        "bks_value",
        "routing_policy",
        "status",
        "source",
    }
    missing = required - set(foodmart.columns)
    if missing:
        raise ValueError(f"Foodmart transcription is missing columns: {sorted(missing)}")
    if len(foodmart) != 42:
        raise ValueError(f"Expected 42 Foodmart Table B.15 rows, found {len(foodmart)}")
    if not foodmart["aisles_S"].eq(8).all():
        raise ValueError("Foodmart Table B.15 transcription must contain only S=8 rows")
    if not foodmart["status"].str.lower().eq("proven optimum").all():
        raise ValueError("Every transcribed Foodmart Table B.15 row must be proven optimal")


def main() -> None:
    required = [WG_RAW, HENN_IDENTIFIERS, FOODMART_B15]
    missing = [str(path) for path in required if not path.exists()]
    if missing:
        raise FileNotFoundError("Missing reference inputs:\n  " + "\n  ".join(missing))

    PROCESSED.mkdir(parents=True, exist_ok=True)
    validate_foodmart_transcription()

    henn = build_henn_reference()
    muter = build_muter_wg_reference()
    henn.to_csv(PROCESSED / "henn_waescher_wg2023.csv", index=False)
    muter.to_csv(PROCESSED / "muter_oencan_wg2023.csv", index=False)

    summary = pd.DataFrame(
        [
            {
                "benchmark_variant": "HennWaescher",
                "rows": len(henn),
                "proven_optima": int(henn["bks_is_opt"].sum()),
                "input_kind": "raw machine-readable",
            },
            {
                "benchmark_variant": "MuterOencanWG",
                "rows": len(muter),
                "proven_optima": int(muter["optimal_bks_is_opt"].sum()),
                "input_kind": "raw machine-readable",
            },
            {
                "benchmark_variant": "Foodmart",
                "rows": 42,
                "proven_optima": 42,
                "input_kind": "manual Table B.15 transcription",
            },
        ]
    )
    summary.to_csv(PROCESSED / "reference_build_summary.csv", index=False)
    print(summary.to_string(index=False))
    print(f"\nPrepared reference data in {PROCESSED}")


if __name__ == "__main__":
    main()
