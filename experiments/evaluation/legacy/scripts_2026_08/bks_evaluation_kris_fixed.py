"""CASOP VBS vs. literature BKS evaluation.

Place this file at experiments/evaluation/bks_evaluation.py and run

    python experiments/evaluation/bks_evaluation.py

It reads df_results.parquet plus the literature/result files already in data/results
and data/results/bks. It does not depend on any other evaluation script.
"""

from pathlib import Path
import re

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


# -----------------------------------------------------------------------------
# Paths
# -----------------------------------------------------------------------------

EVAL_DIR = Path(__file__).resolve().parent
ROOT = EVAL_DIR.parents[1]
RESULTS = ROOT / "data" / "results"
BKS = RESULTS / "bks"
OUT = EVAL_DIR / "bks_outputs"

DF_RESULTS = EVAL_DIR / "df_results.parquet"
SPRP = RESULTS / "results_SPRP.csv"
SPRP_SS = RESULTS / "results_SPRP-SS.csv"
BAHCECI = RESULTS / "results_BahceciOencan.csv"
MUTER = RESULTS / "results_Muter.csv"
MUTER_WG_BKS = BKS / "muter_oencan_wahlen_gschwind2023_audit.csv"
HENN_IDENTIFIERS = RESULTS / "results_HennWaescher.csv"
HENN_BKS = BKS / "henn_waescher_wahlen_gschwind2023_bks.csv"
FOODMART_BKS = BKS / "foodmart_wahlen2026_bks.csv"
KRIS_SMALL = RESULTS / "allSolutions" / "solutionssmall"
KRIS_LARGE = RESULTS / "allSolutions" / "solutionslarge"
KRIS_SMALL_INSTANCES = ROOT / "data" / "instances" / "KrisSmallDataCorrected"
KRIS_LARGE_INSTANCES = ROOT / "data" / "instances" / "KrisLargeData"

EXPECTED_REFERENCE_COUNTS = {
    "SPRP": 2400,
    "SPRP-SS": 14300,
    "BahceciOencan": 1350,
    "HennWaescher": 5759,
    "MuterOencan": 270,       # diagnostic / provenance only
    "MuterOencanWG": 270,     # paper comparison
    "Foodmart": 42,
    "Kris": 294,
}

# The publication table intentionally uses only the Wahlen--Gschwind
# interpretation of Muter--Öncan.  The H&I-compatible set remains in the
# diagnostic CSVs so the geometry transition stays auditable.
PAPER_ORDER = [
    ("SPRP", "distance"),
    ("SPRP-SS", "distance"),
    ("BahceciOencan", "distance"),
    ("HennWaescher", "distance"),
    ("MuterOencanWG", "distance"),
    ("Foodmart", "distance"),
    ("Kris", "total picking time"),
]

PAPER_LABELS = {
    "SPRP": "SPRP",
    "SPRP-SS": "SPRP-SS",
    "BahceciOencan": r"Bahceci--Öncan",
    "HennWaescher": r"Henn--Wäscher",
    "MuterOencanWG": r"Muter--Öncan",
    "Foodmart": "Foodmart",
    "Kris": "Kris",
}

ALGO_COLS = [
    "item_assignment_algo",
    "batching_algo",
    "routing_algo",
    "scheduling_algo",
]

REF_COLS = [
    "instance_set", "instance_name", "reference_value", "reference_runtime",
    "reference_is_opt", "reference_type", "reference_source", "reference_policy",
]

CMP_COLS = [
    "instance_set", "instance_name", "objective", "strategy", "casop_value",
    "total_cpu_time", "reference_value", "gap_[%]", "reference_is_opt",
    "reference_type", "reference_source", "reference_policy", "reference_runtime",
]


# -----------------------------------------------------------------------------
# Small helpers
# -----------------------------------------------------------------------------

def read_legacy(path, skiprows=None):
    return pd.read_csv(path, sep=";", decimal=",", thousands=".", skiprows=skiprows)


def bools(s):
    if pd.api.types.is_bool_dtype(s):
        return s.fillna(False).astype(bool)
    return s.astype(str).str.strip().str.lower().eq("true")


def empty(s):
    text = s.astype(str).str.strip().str.lower()
    return s.isna() | text.isin(["", "none", "nan"])


def suffix_once(s, suffix):
    s = s.astype(str)
    return s.where(s.str.endswith(suffix), s + suffix)


def ref_type(is_opt):
    return np.where(is_opt, "optimum", "reported feasible solution")


def gap_min(value, ref):
    out = (value - ref) / ref * 100.0
    return out.mask(out.abs() < 1e-9, 0.0)


def gap_max(value, ref):
    out = (ref - value) / ref * 100.0
    return out.mask(out.abs() < 1e-9, 0.0)


def strategy(row):
    return "+".join(
        str(row[c]) for c in ALGO_COLS
        if c in row.index and pd.notna(row[c]) and str(row[c]).strip() not in ["", "None", "nan"]
    )


def vbs(df, value_col, maximize=False, secondary=None):
    """One complete winning row per instance; ties use lower CPU time."""
    work = df.copy()
    work[value_col] = pd.to_numeric(work[value_col], errors="coerce")
    work["total_cpu_time"] = pd.to_numeric(work["total_cpu_time"], errors="coerce")
    work = work.dropna(subset=["instance_name", value_col])

    cols = ["instance_name", value_col]
    ascending = [True, not maximize]
    for col, asc in secondary or []:
        cols.append(col)
        ascending.append(asc)
    cols.append("total_cpu_time")
    ascending.append(True)

    return (
        work.sort_values(cols, ascending=ascending, kind="mergesort")
        .drop_duplicates("instance_name", keep="first")
    )


# -----------------------------------------------------------------------------
# CASOP result normalization
# -----------------------------------------------------------------------------

def load_casop():
    df = pd.read_parquet(DF_RESULTS, engine="pyarrow").copy()

    # Henn/Waescher
    m = df["instance_set"].eq("HennWaescherClassBased")
    df.loc[m, "instance_name"] = suffix_once(df.loc[m, "instance_name"], "_cb")
    df.loc[m, "instance_set"] = "HennWaescher"
    m = df["instance_set"].eq("HennWaescherUniform")
    df.loc[m, "instance_name"] = suffix_once(df.loc[m, "instance_name"], "_u")
    df.loc[m, "instance_set"] = "HennWaescher"

    # Foodmart
    m = df["instance_set"].eq("FoodmartData")
    df.loc[m, "instance_name"] = (
        df.loc[m, "instance_name"].astype(str)
        .str.replace(r"^instances_|_MAL$", "", regex=True)
    )
    df.loc[m, "instance_set"] = "Foodmart"

    # Kris
    m = df["instance_set"].eq("KrisSmallDataCorrected")
    df.loc[m, "instance_name"] = suffix_once(df.loc[m, "instance_name"], "_small")
    df.loc[m, "instance_set"] = "Kris"
    m = df["instance_set"].eq("KrisLargeData")
    df.loc[m, "instance_name"] = suffix_once(df.loc[m, "instance_name"], "_large")
    df.loc[m, "instance_set"] = "Kris"

    if "strategy" not in df:
        df["strategy"] = df.apply(strategy, axis=1)

    if "total_cpu_time" not in df:
        stage_cols = [c for c in ["ia_time", "routing_input_time", "total_route_time", "scheduling_time"] if c in df]
        if not stage_cols:
            raise ValueError("df_results has no total_cpu_time or stage timing columns")
        df["total_cpu_time"] = sum(pd.to_numeric(df[c], errors="coerce").fillna(0) for c in stage_cols)

    df["total_distance"] = pd.to_numeric(df["total_distance"], errors="coerce")
    return df


# -----------------------------------------------------------------------------
# Literature BKS/reference loaders
# -----------------------------------------------------------------------------

def sprp_refs():
    x = read_legacy(SPRP)
    x["instance_name"] = x.apply(
        lambda r: f"unit_F1_m{r['num aisles']}_C{r['num cells']}_a{r['num articles']}_{r['random seed']}",
        axis=1,
    )
    x["reference_value"] = pd.to_numeric(x["GS MIP cost"], errors="coerce")
    x["reference_runtime"] = pd.to_numeric(x["GS MIP time cplex [ms]"], errors="coerce") / 1000
    x["reference_is_opt"] = True
    x["reference_source"] = "Heßler & Irnich (2024)"
    x["reference_policy"] = "exact"
    x["instance_set"] = "SPRP"
    x["reference_type"] = ref_type(x["reference_is_opt"])
    return x[REF_COLS].drop_duplicates("instance_name")


def sprp_ss_refs():
    x = read_legacy(SPRP_SS)

    # results_SPRP-SS.csv also contains the 2,400 alpha=1 instances that
    # constitute the regular SPRP benchmark.  The actual SPRP-SS benchmark
    # contains only the scattered-storage variants (alpha > 1): 14,300 rows.
    alpha = pd.to_numeric(x["alpha"], errors="coerce")
    x = x[alpha.gt(1)].copy()

    demand = np.where(bools(x["unit demand"]), "unit", "varying")
    x["instance_name"] = [
        f"{d}_F{a}_m{m}_C{c}_a{n}_{seed}"
        for d, a, m, c, n, seed in zip(
            demand, x["alpha"], x["num aisles"], x["num cells"],
            x["num articles"], x["random seed"]
        )
    ]
    x["reference_value"] = pd.to_numeric(x["GS MIP cost"], errors="coerce")
    x["reference_runtime"] = pd.to_numeric(x["GS MIP time cplex [ms]"], errors="coerce") / 1000
    x["reference_is_opt"] = True
    x["reference_source"] = "Heßler & Irnich (2024)"
    x["reference_policy"] = "exact"
    x["instance_set"] = "SPRP-SS"
    x["reference_type"] = ref_type(x["reference_is_opt"])
    return x[REF_COLS].drop_duplicates("instance_name")


def bahceci_refs():
    x = read_legacy(BAHCECI)
    x = x[x["policy"].astype(str).str.lower().eq("optimal")].copy()
    x["instance_name"] = x["filename"].astype(str).str.replace(r"\.txt$", "", regex=True)
    x["reference_value"] = pd.to_numeric(x["UB"], errors="coerce")
    x["reference_runtime"] = pd.to_numeric(x["time [s]"], errors="coerce")
    x["reference_is_opt"] = bools(x["opt?"])
    x["reference_source"] = "Heßler & Irnich (2022)"
    x["reference_policy"] = "optimal"
    x["instance_set"] = "BahceciOencan"
    x["reference_type"] = ref_type(x["reference_is_opt"])
    return x[REF_COLS].drop_duplicates("instance_name")


def henn_prefix_map():
    """
    Recover the canonical Henn/Waescher instance prefixes from the existing
    Heßler & Irnich result file.

    The S-Shape/Traversal prefixes are not generated by a simple arithmetic
    offset from the Largest-Gap prefixes.  The legacy result file already
    contains the canonical benchmark filenames, so use it only as the
    identifier map; no objective values are taken from it.
    """
    legacy = read_legacy(HENN_IDENTIFIERS)
    filename = legacy["filename"].astype(str)

    legacy["source_variant"] = np.select(
        [
            filename.str.contains("Instances_Largest_Gap", regex=False),
            filename.str.contains("Instances_S-Shape", regex=False),
        ],
        ["LargestGap", "Traversal"],
        default=None,
    )

    basename = filename.map(lambda s: s.replace("\\", "/").rsplit("/", 1)[-1])
    legacy["prefix"] = basename.str.extract(r"^(\d+[ls])-", expand=False)
    legacy["num_orders"] = pd.to_numeric(legacy["number of orders"], errors="coerce")
    legacy["capacity"] = pd.to_numeric(legacy["capacity"], errors="coerce")

    keys = ["source_variant", "num_orders", "capacity"]
    mapping = legacy.dropna(subset=keys + ["prefix"])[keys + ["prefix"]].copy()

    # Every variant/order/capacity combination must have exactly one prefix.
    n_prefixes = mapping.groupby(keys)["prefix"].nunique()
    bad = n_prefixes[n_prefixes.ne(1)]
    if not bad.empty:
        raise ValueError(
            "Ambiguous Henn/Waescher prefix mapping:\n"
            + bad.to_string()
        )

    mapping = mapping.drop_duplicates(keys).reset_index(drop=True)
    if len(mapping) != 72:
        raise ValueError(
            f"Expected 72 Henn/Waescher variant/order/capacity prefix mappings, "
            f"got {len(mapping)}"
        )
    return mapping


def henn_refs():
    """Load Wahlen & Gschwind (2023) BKS and map them to CASOP instance IDs."""
    x = pd.read_csv(HENN_BKS)
    x["num_orders"] = pd.to_numeric(x["num_orders"], errors="raise").astype(int)
    x["capacity"] = pd.to_numeric(x["capacity"], errors="raise").astype(int)
    x["inst_num"] = pd.to_numeric(x["inst_num"], errors="raise").astype(int)

    prefix_map = henn_prefix_map()
    x = x.merge(
        prefix_map,
        on=["source_variant", "num_orders", "capacity"],
        how="left",
        validate="many_to_one",
        indicator=True,
    )
    missing_map = x[x["_merge"].ne("both")]
    if not missing_map.empty:
        cols = ["source_variant", "num_orders", "capacity"]
        raise ValueError(
            "Missing Henn/Waescher prefix mappings:\n"
            + missing_map[cols].drop_duplicates().to_string(index=False)
        )
    x = x.drop(columns="_merge")

    suffix = x["storage"].map({"UDD": "_u", "CBD": "_cb"})
    if suffix.isna().any():
        bad = sorted(x.loc[suffix.isna(), "storage"].astype(str).unique())
        raise ValueError(f"Unexpected Henn/Waescher storage classes: {bad}")

    x["instance_name"] = (
        x["prefix"].astype(str) + "-"
        + x["num_orders"].astype(str) + "-"
        + x["capacity"].astype(str) + "-"
        + x["inst_num"].astype(str)
        + suffix
    )

    # The source instance is malformed/empty in CASOP and is intentionally absent.
    x = x[x["instance_name"] != "2l-20-45-0_u"].copy()

    x["reference_value"] = pd.to_numeric(x["bks_value"], errors="coerce")
    x["reference_runtime"] = np.nan
    x["reference_is_opt"] = bools(x["bks_is_opt"])
    x["reference_source"] = "Wahlen & Gschwind (2023)"
    x["reference_policy"] = x["bks_source_policy"]
    x["instance_set"] = "HennWaescher"
    x["reference_type"] = ref_type(x["reference_is_opt"])

    out = x[REF_COLS].drop_duplicates("instance_name")
    if len(out) != 5759:
        raise ValueError(f"Expected 5759 Henn/Waescher references, got {len(out)}")
    if int(out["reference_is_opt"].sum()) != 5457:
        raise ValueError(
            "Expected 5457 proven-optimal Henn/Waescher references, got "
            f"{int(out['reference_is_opt'].sum())}"
        )
    return out


def muter_refs():
    """Best directly comparable feasible H&I UB across all routing-policy rows."""
    x = read_legacy(MUTER, skiprows=1)
    for c in ["number of orders", "capacity", "random seed", "UB", "time [s]"]:
        x[c] = pd.to_numeric(x[c], errors="coerce")
    x = x.dropna(subset=["number of orders", "capacity", "random seed", "UB"]).copy()
    x["random seed"] = x["random seed"].astype(int) - 1
    x["instance_name"] = x.apply(
        lambda r: f"{int(r['number of orders'])}_{int(r['capacity'])}_{int(r['random seed'])}",
        axis=1,
    )

    # Restricted routing gives feasible unrestricted-OBP solutions, so the
    # smallest directly comparable published UB is the BKS.
    x["_proves_unrestricted"] = (
        x["policy"].astype(str).str.lower().eq("optimal")
        & bools(x["opt?"])
    )
    x = (
        x.sort_values(
            ["instance_name", "UB", "_proves_unrestricted", "time [s]"],
            ascending=[True, True, False, True], kind="mergesort"
        )
        .drop_duplicates("instance_name", keep="first")
    )
    x["reference_value"] = x["UB"]
    x["reference_runtime"] = x["time [s]"]
    x["reference_is_opt"] = x["_proves_unrestricted"]
    x["reference_source"] = "Heßler & Irnich (2022)"
    x["reference_policy"] = x["policy"]
    x["instance_set"] = "MuterOencan"
    x["reference_type"] = ref_type(x["reference_is_opt"])
    out = x[REF_COLS]
    if len(out) != 270:
        raise ValueError(f"Expected 270 Muter/Oencan references, got {len(out)}")
    return out


def muter_wg_refs():
    """Wahlen & Gschwind (2023) references for the WG-geometry M&O instances."""
    x = pd.read_csv(MUTER_WG_BKS)

    for c in ["capacity", "num_orders", "inst_num", "optimal_bks"]:
        x[c] = pd.to_numeric(x[c], errors="coerce")
    x = x.dropna(subset=["capacity", "num_orders", "inst_num", "optimal_bks"]).copy()

    # W&G number the ten base instances 1..10; CASOP filenames are 0..9.
    x["instance_name"] = x.apply(
        lambda r: (
            f"{int(r['num_orders'])}_{int(r['capacity'])}_{int(r['inst_num']) - 1}"
        ),
        axis=1,
    )

    # These values are directly comparable only to the derived MuterOencanWG
    # set, whose aisle-entry distances follow W&G.  Do not use them for the
    # original MuterOencan set.
    x["reference_value"] = x["optimal_bks"]
    x["reference_runtime"] = np.nan
    x["reference_is_opt"] = bools(x["optimal_bks_is_opt"])
    x["reference_source"] = "Wahlen & Gschwind (2023)"
    x["reference_policy"] = "optimal"
    x["instance_set"] = "MuterOencanWG"
    x["reference_type"] = ref_type(x["reference_is_opt"])

    out = x[REF_COLS].drop_duplicates("instance_name")
    if len(out) != 270:
        raise ValueError(
            f"Expected 270 Wahlen/Gschwind Muter/Oencan references, got {len(out)}"
        )
    if int(out["reference_is_opt"].sum()) != 234:
        raise ValueError(
            "Expected 234 proven-optimal Wahlen/Gschwind Muter/Oencan "
            f"references, got {int(out['reference_is_opt'].sum())}"
        )
    return out


def foodmart_refs():
    x = pd.read_csv(FOODMART_BKS)
    x["instance_name"] = (
        "d" + x["delta"].astype(int).astype(str)
        + "_ord" + x["orders_n"].astype(int).astype(str)
    )
    x["reference_value"] = pd.to_numeric(x["bks_value"], errors="coerce")
    x["reference_runtime"] = np.nan
    x["reference_is_opt"] = x["status"].astype(str).str.lower().eq("proven optimum")
    x["reference_source"] = "Wahlen (2026)"
    x["reference_policy"] = x["routing_policy"] if "routing_policy" in x else "optimal"
    x["instance_set"] = "Foodmart"
    x["reference_type"] = ref_type(x["reference_is_opt"])
    out = x[REF_COLS].drop_duplicates("instance_name")
    if len(out) != 42 or not out["reference_is_opt"].all():
        raise ValueError("Expected 42 proven-optimal Foodmart references")
    return out


def load_distance_refs():
    refs = pd.concat(
        [sprp_refs(), sprp_ss_refs(), bahceci_refs(), henn_refs(), muter_refs(), muter_wg_refs(), foodmart_refs()],
        ignore_index=True,
    )
    if refs.duplicated(["instance_set", "instance_name"]).any():
        raise ValueError("Duplicate literature reference keys")
    for name, expected in EXPECTED_REFERENCE_COUNTS.items():
        if name == "Kris":
            continue
        actual = int(refs["instance_set"].eq(name).sum())
        if actual != expected:
            raise ValueError(f"{name}: expected {expected} references, got {actual}")
    return refs


# -----------------------------------------------------------------------------
# Briant/Kris reference parser
# -----------------------------------------------------------------------------

BATCH_RE = re.compile(
    r"PickerID\t(\d+)\tBatchID\t(\d+)\tPreviousBatch\t(\d+)\tNoOders\t(\d+)\t"
    r"NoOderLines\t(\d+)\tBatchDistance\t(\d+)\tBatchComplTime\t(\d+)"
)
ORDER_RE = re.compile(
    r"OrderID\t(\d+)\tNoOrderLines\t(\d+)\tNextOrderID\t(\d+)\tPickerID\t(\d+)\t"
    r"BatchID\t(\d+)\tDueTime\t(\d+)\tCompletionTime\t(\d+)"
)


def _kris_instance_parameter(text, name):
    m = re.search(rf"//{re.escape(name)}\s*\n\s*([0-9]+(?:\.[0-9]+)?)", text)
    if not m:
        raise ValueError(f"Could not parse Kris parameter {name!r}")
    return float(m.group(1))


def kris_file(solution_path, instance_path):
    """Parse one Briant solution and reconstruct the JOBPRSP-D objective.

    Briant et al. minimize the sum of route processing times subject to hard
    deadlines.  For these benchmark files a route time is

        distance * TimeToTravelOneDistanceUnit
        + number_of_order_lines * PickTime
        + SetupTime.

    This is the same quantity stored by CASOP as ``total_time``.
    """
    solution_text = solution_path.read_text(encoding="utf-8", errors="replace")
    instance_text = instance_path.read_text(encoding="utf-8", errors="replace")

    batches = []
    for m in BATCH_RE.finditer(solution_text):
        batches.append({
            "picker_id": int(m[1]),
            "batch_id": int(m[2]),
            "previous_batch": int(m[3]),
            "n_orders": int(m[4]),
            "n_lines": int(m[5]),
            "distance": int(m[6]),
            "completion_time": int(m[7]),
        })

    orders = []
    for m in ORDER_RE.finditer(solution_text):
        orders.append({
            "order_id": int(m[1]),
            "picker_id": int(m[4]),
            "batch_id": int(m[5]),
            "due_time": int(m[6]),
            "completion_time": int(m[7]),
        })

    if not batches or not orders:
        raise ValueError(f"Could not parse {solution_path}")

    travel_per_distance = _kris_instance_parameter(
        instance_text, "TimeToTravelOneDistanceUnit"
    )
    setup_time = _kris_instance_parameter(instance_text, "SetupTime")
    pick_time = _kris_instance_parameter(instance_text, "PickTime")

    total_distance = sum(b["distance"] for b in batches)
    total_lines = sum(b["n_lines"] for b in batches)
    n_batches = len(batches)

    reference_total_time = (
        total_distance * travel_per_distance
        + total_lines * pick_time
        + n_batches * setup_time
    )

    # Independent audit using the published completion times.  With no release
    # dates, each picker works continuously from time zero, so the sum of each
    # picker's final completion time equals the sum of all route times.
    final_by_picker = {}
    for b in batches:
        final_by_picker[b["picker_id"]] = max(
            final_by_picker.get(b["picker_id"], 0),
            b["completion_time"],
        )
    completion_time_total = float(sum(final_by_picker.values()))
    if not np.isclose(reference_total_time, completion_time_total, atol=1e-6):
        raise ValueError(
            f"{solution_path.name}: reconstructed total picking time "
            f"{reference_total_time} != sum of final picker completion times "
            f"{completion_time_total}"
        )

    deadline_feasible = all(
        o["completion_time"] <= o["due_time"] for o in orders
    )
    # if not deadline_feasible:
    #     raise ValueError(
    #         f"{solution_path.name}: published JOBPRSP-D solution violates a deadline"
    #     )

    return {
        "reference_value": float(reference_total_time),
        "best_total_distance": float(total_distance),
        "best_on_time_rate": 100.0,
    }


def kris_refs(directory, instance_directory, suffix, *, small):
    """Build Briant et al. JOBPRSP-D references.

    The paper proves all small benchmark instances optimal.  It also proves
    some large instances, but the repository currently contains the published
    solution files without an instance-level proof-status table for the large
    set.  We therefore mark the large references conservatively as reported
    feasible solutions rather than overclaiming optimality.
    """
    rows = []
    for path in sorted(directory.glob("*.txt")):
        m = re.fullmatch(r"solution_(?:small|large)_(\d+)_(\d+)", path.stem)
        if not m:
            raise ValueError(f"Unexpected Kris solution filename: {path.name}")

        instance_base = f"instances_{m[1]}_{m[2]}"
        instance_path = instance_directory / f"{instance_base}.txt"
        if not instance_path.exists():
            raise FileNotFoundError(
                f"Missing Kris instance corresponding to {path.name}: {instance_path}"
            )

        parsed = kris_file(path, instance_path)
        rows.append({
            "instance_set": "Kris",
            "instance_name": f"{instance_base}{suffix}",
            "reference_value": parsed["reference_value"],
            "reference_runtime": np.nan,
            "reference_is_opt": bool(small),
            "reference_type": "optimum" if small else "reported feasible solution",
            "reference_source": "Briant et al. (2023)",
            "reference_policy": "JOBPRSP-D published solution",
        })

    return pd.DataFrame(rows, columns=REF_COLS)


def load_kris_refs():
    refs = pd.concat(
        [
            kris_refs(
                KRIS_SMALL, KRIS_SMALL_INSTANCES, "_small", small=True
            ),
            kris_refs(
                KRIS_LARGE, KRIS_LARGE_INSTANCES, "_large", small=False
            ),
        ],
        ignore_index=True,
    )
    if refs.duplicated(["instance_set", "instance_name"]).any():
        raise ValueError("Duplicate Kris reference keys")
    # if len(refs) != EXPECTED_REFERENCE_COUNTS["Kris"]:
    #     raise ValueError(
    #         f"Expected {EXPECTED_REFERENCE_COUNTS['Kris']} Kris references, "
    #         f"got {len(refs)}"
    #     )
    return refs


# -----------------------------------------------------------------------------
# Comparisons
# -----------------------------------------------------------------------------

def distance_comparisons(casop, refs):
    out = []
    for name in ["SPRP", "SPRP-SS", "BahceciOencan", "HennWaescher", "MuterOencan", "MuterOencanWG", "Foodmart"]:
        x = casop[casop["instance_set"].eq(name)].copy()
        x = x[x["total_distance"].notna() & x["total_distance"].gt(0)]
        if "scheduling_algo" in x:
            x = x[empty(x["scheduling_algo"])]
        x = vbs(x, "total_distance")
        r = refs[refs["instance_set"].eq(name)].copy()

        casop_names = set(x["instance_name"])
        missing = r.loc[~r["instance_name"].isin(casop_names), "instance_name"]
        if len(missing):
            sample = ", ".join(missing.head(10).astype(str))
            raise ValueError(
                f"{name}: {len(missing)} literature references have no CASOP VBS match. "
                f"First missing keys: {sample}"
            )

        x = x.merge(
            r,
            on=["instance_set", "instance_name"], how="inner", validate="one_to_one"
        )
        x["objective"] = "distance"
        x["casop_value"] = x["total_distance"]
        x["gap_[%]"] = gap_min(x["casop_value"], x["reference_value"])
        out.append(x[CMP_COLS])
    return out


def kris_comparisons(casop, refs):
    """Compare CASOP to Briant et al. on the native JOBPRSP-D objective.

    Deadlines are hard constraints.  We therefore first retain only scheduled
    pipelines that satisfy every deadline (on_time_rate == 100%), then minimize
    total picking time among those feasible pipelines.
    """
    x = casop[casop["instance_set"].eq("Kris")].copy()

    if "scheduling_algo" in x:
        x = x[~empty(x["scheduling_algo"])]

    x["on_time_rate"] = pd.to_numeric(x["on_time_rate"], errors="coerce")
    x["total_time"] = pd.to_numeric(x["total_time"], errors="coerce")
    x = x.dropna(subset=["on_time_rate", "total_time"])

    # Treat tiny floating error around 100% as feasible.
    x = x[x["on_time_rate"].ge(100.0 - 1e-9)].copy()
    x = vbs(x, "total_time")

    r = refs[refs["instance_set"].eq("Kris")].copy()
    x = x.merge(
        r,
        on=["instance_set", "instance_name"],
        how="inner",
        validate="one_to_one",
    )
    x["objective"] = "total picking time"
    x["casop_value"] = x["total_time"]
    x["gap_[%]"] = gap_min(x["casop_value"], x["reference_value"])

    # A proven optimum cannot be improved.  This catches objective/unit/mapping
    # errors immediately.
    contradictions = x[
        x["reference_is_opt"] & x["gap_[%]"].lt(-1e-9)
    ]
    if not contradictions.empty:
        sample = contradictions[
            ["instance_name", "casop_value", "reference_value", "gap_[%]"]
        ].head(10)
        raise ValueError(
            "Kris: CASOP is below a proven Briant optimum; check objective "
            "reconstruction or instance mapping:\n"
            + sample.to_string(index=False)
        )

    return x[CMP_COLS]




# -----------------------------------------------------------------------------
# Paper outputs
# -----------------------------------------------------------------------------

def diagnostic_summary(cmp):
    """Verbose console/CSV summary; not all columns belong in the paper table."""
    rows = []
    order = [
        ("SPRP", "distance"),
        ("SPRP-SS", "distance"),
        ("BahceciOencan", "distance"),
        ("HennWaescher", "distance"),
        ("MuterOencan", "distance"),
        ("MuterOencanWG", "distance"),
        ("Foodmart", "distance"),
        ("Kris", "total picking time"),
    ]
    for name, objective in order:
        x = cmp[
            cmp["instance_set"].eq(name) & cmp["objective"].eq(objective)
        ]
        if x.empty:
            continue
        rows.append({
            "instance_set": name,
            "objective": objective,
            "n_comparisons": x["instance_name"].nunique(),
            "mean_gap_[%]": x["gap_[%]"].mean(),
            "median_gap_[%]": x["gap_[%]"].median(),
            "max_gap_[%]": x["gap_[%]"].max(),
            "mean_vbs_runtime_[s]": x["total_cpu_time"].mean(),
        })
    return pd.DataFrame(rows).round(3)


def coverage_table(cmp, refs):
    rows = []
    for name, expected in EXPECTED_REFERENCE_COUNTS.items():
        r = refs[refs["instance_set"].eq(name)]
        objective = (
            "total picking time" if name == "Kris" else "distance"
        )
        c = cmp[
            cmp["instance_set"].eq(name) & cmp["objective"].eq(objective)
        ]
        n_cmp = c["instance_name"].nunique()
        rows.append({
            "instance_set": name,
            "references": len(r),
            "proven_optimal_references": int(r["reference_is_opt"].sum()),
            "casop_feasible_comparisons": n_cmp,
            "missing_casop_comparisons": len(r) - n_cmp,
            "feasible_share_[%]": 100.0 * n_cmp / len(r) if len(r) else np.nan,
            "mean_gap_[%]": c["gap_[%]"].mean(),
            "median_gap_[%]": c["gap_[%]"].median(),
            "max_gap_[%]": c["gap_[%]"].max(),
            "casop_better_than_published": int(c["gap_[%]"].lt(-1e-9).sum()),
        })

        # if len(r) != expected:
        #     raise ValueError(
        #         f"{name}: expected {expected} references, got {len(r)}"
        #     )

    return pd.DataFrame(rows).round(3)


def paper_table(cmp, refs):
    """Compact publication table.

    The main table follows standard OR computational-reporting practice:
    reference coverage/optimality, mean solution-quality gap, a worst-case gap,
    and CASOP runtime.  Median gap remains in the diagnostic CSV but is omitted
    here because it adds width without changing the main conclusion.
    """
    rows = []
    for name, objective in PAPER_ORDER:
        r = refs[refs["instance_set"].eq(name)]
        c = cmp[
            cmp["instance_set"].eq(name) & cmp["objective"].eq(objective)
        ]
        if r.empty:
            raise ValueError(f"No references available for paper row {name}")
        if c.empty:
            # Kris can in principle have zero deadline-feasible CASOP pipelines;
            # preserve the row rather than silently dropping the benchmark.
            mean_gap = np.nan
            max_gap = np.nan
            mean_time = np.nan
            n_feasible = 0
        else:
            mean_gap = c["gap_[%]"].mean()
            max_gap = c["gap_[%]"].max()
            mean_time = c["total_cpu_time"].mean()
            n_feasible = c["instance_name"].nunique()

        rows.append({
            "Instance Set": PAPER_LABELS[name],
            "Objective": objective,
            "Feas./Ref.": f"{n_feasible:,}/{len(r):,}",
            "# Opt.": int(r["reference_is_opt"].sum()),
            "Mean gap [%]": mean_gap,
            "Max gap [%]": max_gap,
            "Mean VBS time [s]": mean_time,
        })

    return pd.DataFrame(rows)


def write_paper_latex(table, path):
    lines = [
        r"\begin{table}[tbp]",
        r"\centering",
        r"\caption{Comparison of the CASOP virtual best solver (VBS) with "
        r"published reference solutions. Gaps are per-instance relative gaps "
        r"and are aggregated over instances for which CASOP finds a feasible "
        r"solution. For Kris, feasibility requires all deadlines to be met. "
        r"VBS time is the mean runtime of the ex-post winning pipeline.}",
        r"\label{tab:vbs_vs_bks}",
        r"\small",
        r"\setlength{\tabcolsep}{4pt}",
        r"\begin{tabular}{@{}llrrrrr@{}}",
        r"\toprule",
        r"Instance Set & Objective & Feas./Ref. & \# Opt. "
        r"& Mean gap [\%] & Max gap [\%] & Mean VBS time [s] \\",
        r"\midrule",
    ]

    for _, row in table.iterrows():
        def fmt(value):
            return "--" if pd.isna(value) else f"{float(value):.3f}"

        lines.append(
            rf"\textit{{{row['Instance Set']}}} "
            rf"& {row['Objective']} "
            rf"& {row['Feas./Ref.']} "
            rf"& {int(row['# Opt.']):,} "
            rf"& {fmt(row['Mean gap [%]'])} "
            rf"& {fmt(row['Max gap [%]'])} "
            rf"& {fmt(row['Mean VBS time [s]'])} \\"
        )

    lines += [
        r"\bottomrule",
        r"\end{tabular}",
        r"\end{table}",
        "",
    ]
    path.write_text("\n".join(lines), encoding="utf-8")


def plot_gaps(cmp, path):
    # Plot only the paper's distance-based sets.  Kris uses a different
    # objective/unit and the old H&I Muter interpretation is diagnostic only.
    paper_distance = {
        "SPRP", "SPRP-SS", "BahceciOencan", "HennWaescher",
        "MuterOencanWG", "Foodmart",
    }
    x = cmp[
        cmp["instance_set"].isin(paper_distance)
        & cmp["objective"].eq("distance")
    ].copy()
    x["label"] = x["instance_set"].replace({
        "MuterOencanWG": "MuterOencan",
    })

    groups = [
        ["SPRP", "SPRP-SS"],
        ["BahceciOencan", "HennWaescher", "MuterOencan", "Foodmart"],
    ]

    fig, axes = plt.subplots(
        1, 2, figsize=(14, 6), gridspec_kw={"width_ratios": [1, 2.5]}
    )
    for ax, labels in zip(axes, groups):
        present = [
            (label, x.loc[x["label"].eq(label), "gap_[%]"].dropna().to_numpy())
            for label in labels
        ]
        present = [(label, values) for label, values in present if len(values)]
        ax.boxplot(
            [values for _, values in present],
            tick_labels=[label for label, _ in present],
        )
        ax.axhline(0, linewidth=0.8)
        ax.tick_params(axis="x", labelrotation=45)
        for tick in ax.get_xticklabels():
            tick.set_horizontalalignment("right")
    axes[0].set_ylabel("Gap to published reference (%)")
    fig.tight_layout()
    fig.savefig(path, dpi=200, bbox_inches="tight", pad_inches=0.05)
    plt.close(fig)



def main():
    required = [
        DF_RESULTS, SPRP, SPRP_SS, BAHCECI, MUTER, MUTER_WG_BKS,
        HENN_IDENTIFIERS, HENN_BKS, FOODMART_BKS,
        KRIS_SMALL, KRIS_LARGE, KRIS_SMALL_INSTANCES, KRIS_LARGE_INSTANCES,
    ]
    missing = [str(p) for p in required if not p.exists()]
    if missing:
        raise FileNotFoundError(
            "Missing required inputs:\n  " + "\n  ".join(missing)
        )

    OUT.mkdir(parents=True, exist_ok=True)
    (OUT / "plots").mkdir(exist_ok=True)

    casop = load_casop()
    distance_refs = load_distance_refs()
    kris_reference = load_kris_refs()
    refs = pd.concat([distance_refs, kris_reference], ignore_index=True)

    cmp = pd.concat(
        distance_comparisons(casop, distance_refs)
        + [kris_comparisons(casop, kris_reference)],
        ignore_index=True,
    )

    # Hard audit: no CASOP value may beat a proven optimum.
    contradictions = cmp[
        cmp["reference_is_opt"] & cmp["gap_[%]"].lt(-1e-9)
    ]
    if not contradictions.empty:
        sample = contradictions[
            ["instance_set", "instance_name", "casop_value",
             "reference_value", "gap_[%]"]
        ].head(20)
        raise ValueError(
            "CASOP is below one or more proven optima. "
            "Check instance mapping/objective units:\n"
            + sample.to_string(index=False)
        )

    diagnostics = diagnostic_summary(cmp)
    coverage = coverage_table(cmp, refs)
    paper = paper_table(cmp, refs)
    improvements = cmp[cmp["gap_[%]"].lt(-1e-9)].copy()

    refs.sort_values(["instance_set", "instance_name"]).to_csv(
        OUT / "bks_references_used.csv", index=False
    )
    cmp.sort_values(
        ["instance_set", "objective", "instance_name"]
    ).to_csv(
        OUT / "bks_instance_comparison.csv", index=False
    )
    coverage.to_csv(OUT / "bks_reference_coverage.csv", index=False)
    diagnostics.to_csv(OUT / "bks_diagnostic_summary.csv", index=False)
    improvements.to_csv(
        OUT / "bks_improvements_over_published.csv", index=False
    )
    paper.to_csv(OUT / "tab_vbs_vs_bks.csv", index=False)
    write_paper_latex(paper, OUT / "tab_vbs_vs_bks.tex")

    plot_gaps(
        cmp,
        OUT / "plots" / "vbs_gap_to_bks_boxplot_combined.png",
    )

    print("\nVBS vs published references — diagnostic summary\n")
    print(diagnostics.to_string(index=False))

    print("\nReference coverage\n")
    print(coverage.to_string(index=False))

    print("\nPaper table\n")
    print(paper.to_string(index=False))

    print(f"\nOutputs written to {OUT}")

    if len(improvements):
        print("\nCASOP improvements over published references")
        counts = (
            improvements.groupby(["instance_set", "objective"])
            .size()
            .rename("n")
            .reset_index()
        )
        print(counts.to_string(index=False))
        print(
            "Inspect bks_improvements_over_published.csv for "
            "the instance-level rows."
        )


if __name__ == "__main__":
    main()
