"""
Kris (Briant et al., JOBPRSP-D) validation, reported separately from the
distance-based instance sets.

Why separate: their objective is TOTAL TIME (sum over routes of setup +
picking + travel) subject to HARD deadlines. The other six sets have an
unconstrained distance objective. Mixing them in one table would put a
constrained and an unconstrained problem under the same columns.

Our pipelines do not enforce deadlines, so a solution may be inadmissible in
their problem. We therefore report how many instances yield a deadline-feasible
schedule, measure the gap only on those, and quantify how far the infeasible
ones miss via a relaxation factor (max completion / deadline), which needs no
invented penalty weight.

Requires two columns in the pipeline results frame:
  total_time    sum(processing_time) + setup * n_routes
  max_lateness  already emitted by ResultAggregationDueDate

Add total_time in ResultAggregationDueDate.run():
    setup = self.resources.resources[0].tour_setup_time
    summary["total_time"] = float(
        df_jobs["processing_time"].sum() + setup * len(df_jobs)
    )

Optional, for the relaxation factor:
    summary["max_stretch"] = float((due_eval["completion_time"]
                                    / due_eval["due_date"]).max())
"""

import re
from pathlib import Path

import numpy as np
import pandas as pd

# A schedule counts as feasible when no order finishes after its deadline.
LATENESS_TOL = 1e-6
GAP_TOL = 1e-6

# Published objective columns, in order of preference. OptimalValue marks a
# proven optimum. UBILS is the van Gils ILS result; the authors' own upper
# bound may appear under one of the other names depending on the sheet.
OPT_COL = "OptimalValue"
UB_COLS = ["UB", "UBCGH", "UpperBound", "UpperBoundInit", "UBILS"]


# =============================================================================
# Reference side: published solution files and detailed result tables
# =============================================================================

def parse_solution_file(filepath: Path) -> dict:
    """
    Parse one published solution file.

    Their objective is total time, but the files report completion times, not
    durations. Each batch line carries PreviousBatch, so a batch's duration is
    its completion time minus its predecessor's completion time on the same
    picker, and its own completion time when it is first (no release dates).
    """
    text = Path(filepath).read_text()

    batches = []
    for m in re.finditer(
        r"PickerID\t(\d+)\tBatchID\t(\d+)\tPreviousBatch\t(\d+)\tNoOders\t(\d+)"
        r"\tNoOderLines\t(\d+)\tBatchDistance\t(\d+)\tBatchComplTime\t(\d+)",
        text,
    ):
        batches.append({
            "picker_id": int(m[1]),
            "batch_id": int(m[2]),
            "previous_batch": int(m[3]),
            "distance": int(m[6]),
            "completion_time": int(m[7]),
        })

    if not batches:
        raise ValueError(f"No batch lines parsed from {filepath}")

    # Key on (picker, batch): BatchID may restart per picker, and a route's
    # predecessor is always on the same picker, so this is correct either way.
    completion_by_batch = {
        (b["picker_id"], b["batch_id"]): b["completion_time"] for b in batches
    }
    total_time = 0
    for b in batches:
        prev = b["previous_batch"]
        prev_completion = (
            completion_by_batch.get((b["picker_id"], prev), 0) if prev != 0 else 0
        )
        total_time += b["completion_time"] - prev_completion

    return {
        "best_total_time": float(total_time),
        "best_total_distance": float(sum(b["distance"] for b in batches)),
        "best_makespan": float(max(b["completion_time"] for b in batches)),
        "best_n_batches": len(batches),
        "n_pickers": len({b["picker_id"] for b in batches}),
    }


def parse_solution_dir(directory: Path, suffix: str, glob: str = "*.txt") -> pd.DataFrame:
    rows = []
    for fp in sorted(Path(directory).glob(glob)):
        parts = fp.stem.split("_")
        row = parse_solution_file(fp)
        row["instance_name"] = f"instances_{parts[2]}_{parts[3]}{suffix}"
        rows.append(row)
    return pd.DataFrame(rows)


def load_published_results(results_dir: Path) -> pd.DataFrame:
    """
    Read the CSVs written by parse_briant_results.py.

    Sheets whose name contains noStrenghTour are the ablation from the paper's
    Section 6.4, a deliberately weaker configuration reported to justify a
    design choice. They are not reference results and are skipped.
    """
    results_dir = Path(results_dir)
    if not results_dir.is_dir():
        print(f"[Kris] no published-results dir at {results_dir}; "
              f"falling back to solution files only")
        return pd.DataFrame()

    frames = []
    for path in sorted(results_dir.glob("*.csv")):
        if "nostrenghtour" in path.name.lower():
            continue
        d = pd.read_csv(path)
        d.columns = [str(c).strip() for c in d.columns]
        if not {"InstanceId", "Replication"}.issubset(d.columns):
            print(f"[Kris] skipping {path.name}: no InstanceId/Replication columns")
            continue

        suffix = "_small" if path.name.lower().startswith("small") else "_large"
        d["instance_name"] = (
            "instances_"
            + d["InstanceId"].astype("Int64").astype(str)
            + "_"
            + d["Replication"].astype("Int64").astype(str)
            + suffix
        )
        frames.append(d)
        print(f"[Kris] published results: {path.name} rows={len(d)}")

    if not frames:
        return pd.DataFrame()

    pub = pd.concat(frames, ignore_index=True)

    # Reference value: the proven optimum when the source reports one,
    # otherwise the best published upper bound across the columns present.
    ub_present = [c for c in UB_COLS if c in pub.columns]
    if ub_present:
        best_ub = pub[ub_present].astype(float).min(axis=1)
    else:
        best_ub = pd.Series(np.nan, index=pub.index)

    if OPT_COL in pub.columns:
        opt = pd.to_numeric(pub[OPT_COL], errors="coerce")
    else:
        opt = pd.Series(np.nan, index=pub.index)

    pub["reference_value"] = opt.where(opt.notna(), best_ub)
    pub["reference_type"] = np.where(
        opt.notna(), "optimum", "reported feasible solution"
    )
    pub["reference_runtime"] = (
        pd.to_numeric(pub["Cpu(s)"], errors="coerce") if "Cpu(s)" in pub.columns
        else np.nan
    )

    keep = ["instance_name", "reference_value", "reference_type", "reference_runtime"]
    if "NbBatchesInSol" in pub.columns:
        keep.append("NbBatchesInSol")
    return pub[keep].dropna(subset=["reference_value"])


def build_kris_reference(
    small_dir: Path,
    large_dir: Path,
    published_dir: Path,
) -> pd.DataFrame:
    """Reference frame: published tables where available, solution files otherwise."""
    sol = pd.concat(
        [
            parse_solution_dir(small_dir, "_small"),
            parse_solution_dir(large_dir, "_large"),
        ],
        ignore_index=True,
    )
    sol["reference_value"] = sol["best_total_time"]
    sol["reference_type"] = "reported feasible solution"
    sol["reference_runtime"] = np.nan

    pub = load_published_results(published_dir)
    if pub.empty:
        return sol

    ref = sol.merge(pub, on="instance_name", how="left", suffixes=("_sol", "_pub"))
    has_pub = ref["reference_value_pub"].notna()

    ref["reference_value"] = np.where(
        has_pub, ref["reference_value_pub"], ref["reference_value_sol"]
    )
    ref["reference_type"] = np.where(
        has_pub, ref["reference_type_pub"], ref["reference_type_sol"]
    )
    ref["reference_runtime"] = np.where(
        has_pub, ref["reference_runtime_pub"], ref["reference_runtime_sol"]
    )

    # Cross-check: the derived total time should match a published optimum.
    proven = ref["reference_type"] == "optimum"
    if proven.any():
        signed = (
            (ref.loc[proven, "best_total_time"] - ref.loc[proven, "reference_value"])
            / ref.loc[proven, "reference_value"]
        ) * 100
        n_off = int((signed.abs() > 1.0).sum())
        if n_off:
            median = signed.median()
            direction = (
                "derived value too HIGH: the PreviousBatch subtraction absorbs idle "
                "time between consecutive batches into route durations. Affects this "
                "cross-check only, not the reported gap."
                if median > 0 else
                "derived value too LOW: their route duration includes something the "
                "completion-time differences do not, most likely setup. The same "
                "doubt then applies to the pipeline's own total_time."
            )
            print(
                f"[Kris] NOTE: on {n_off} of {int(proven.sum())} proven-optimal "
                f"instances the total time derived from the solution files differs "
                f"from the published optimum by more than 1% "
                f"(median {median:+.2f}%, range {signed.min():+.2f}% to "
                f"{signed.max():+.2f}%).\n"
                f"       {direction}"
            )

    drop = [c for c in ref.columns if c.endswith(("_sol", "_pub"))]
    return ref.drop(columns=drop)


# =============================================================================
# Pipeline side: feasibility and VBS on total time
# =============================================================================

def build_kris_vbs(
    df: pd.DataFrame, reference: pd.DataFrame
) -> tuple[pd.DataFrame, pd.DataFrame] | None:
    """
    Per-instance VBS on total time, restricted to deadline-feasible schedules.

    A pipeline with a lower total time that misses a deadline is not a better
    solution to their problem, it is a solution to a less constrained one, so
    infeasible pipelines never win the selection.
    """
    if "total_time" not in df.columns:
        print(
            "[Kris] SKIPPED: the results frame has no 'total_time' column. "
            "Emit it from ResultAggregationDueDate as "
            "sum(processing_time) + setup * n_routes, then rebuild the cache."
        )
        return None

    kris = df[df["instance_set"] == "Kris"].copy()
    kris = kris[kris["scheduling_algo"].notna()]
    kris["total_time"] = pd.to_numeric(kris["total_time"], errors="coerce")
    kris = kris[kris["total_time"] > 0]

    kris["max_lateness"] = pd.to_numeric(kris["max_lateness"], errors="coerce")
    kris["feasible"] = kris["max_lateness"] <= LATENESS_TOL

    kris = kris.merge(reference, on="instance_name", how="left")
    kris["gap_[%]"] = (
        (kris["total_time"] - kris["reference_value"]) / kris["reference_value"]
    ) * 100

    if "max_stretch" in kris.columns and pd.to_numeric(
        kris["max_stretch"], errors="coerce"
    ).notna().any():
        kris["relaxation"] = pd.to_numeric(kris["max_stretch"], errors="coerce")
        print("[Kris] relaxation basis: max completion / deadline")
    else:
        # Ex-post fallback. The per-order completion times and due dates are not
        # kept in the summary, so the exact stretch cannot be recomputed. We
        # express the worst overshoot as a fraction of the REFERENCE schedule
        # length, which is fixed per instance. Dividing by our own makespan
        # would let a worse solution score better, since its longer schedule
        # enlarges the denominator. 1.0 is the feasibility boundary either way.
        span = pd.Series(np.nan, index=kris.index)
        basis = "own makespan"
        if "best_makespan" in kris.columns:
            span = pd.to_numeric(kris["best_makespan"], errors="coerce")
            basis = "reference makespan"
        if span.notna().sum() == 0:
            span = pd.to_numeric(kris["makespan"], errors="coerce")
            basis = "own makespan"
        kris["relaxation"] = np.where(
            span > 0, 1.0 + kris["max_lateness"] / span, np.nan
        )
        print(f"[Kris] relaxation basis: 1 + max_lateness / {basis} (ex-post proxy)")

    # Feasible first, then lowest total time. Total time does not depend on the
    # assignment, so for a fixed batching and routing every scheduler ties on
    # the objective. Breaking that tie on slack (lowest max_lateness) picks the
    # most robust schedule and is deterministic, unlike a CPU-time tie-break.
    ordered = kris.sort_values(
        ["feasible", "total_time", "max_lateness", "total_cpu_time"],
        ascending=[False, True, True, True],
    )
    vbs = ordered.groupby("instance_name").first().reset_index()
    vbs["instance_set"] = "Kris"

    # The scheduler cannot change total time, so the pipeline that matters for
    # the winner distribution is the batching-and-routing part. The scheduler
    # dimension is reported separately as feasibility coverage.
    vbs["strategy_full"] = vbs["strategy"]
    vbs["strategy"] = vbs["strategy"].str.rsplit("+", n=1).str[0]
    return vbs, kris


def build_scheduler_feasibility(kris: pd.DataFrame) -> pd.DataFrame:
    """
    Per-scheduler feasibility coverage.

    Since every scheduler yields the same total time for a fixed batching and
    routing, how often a scheduler wins the VBS selection is meaningless. What
    distinguishes them is whether the deadlines hold, so we report the share of
    instances on which each scheduler produces at least one feasible schedule.
    """
    rows = []
    for algo, grp in kris.groupby("scheduling_algo"):
        covered = grp["instance_name"].nunique()
        feasible = grp.loc[grp["feasible"], "instance_name"].nunique()
        rows.append({
            "scheduler": algo,
            "n_instances": covered,
            "n_feasible": feasible,
            "share_feasible_[%]": round(100 * feasible / covered, 1) if covered else np.nan,
        })
    return (
        pd.DataFrame(rows)
        .sort_values("share_feasible_[%]", ascending=False)
        .reset_index(drop=True)
    )


def build_kris_table(vbs: pd.DataFrame, n_evaluated: int) -> pd.DataFrame:
    """One-row summary for the Kris validation table."""
    feasible = vbs["feasible"] == True  # noqa: E712
    referenced = vbs["reference_value"].notna()
    is_opt = vbs["reference_type"] == "optimum"

    scored = feasible & referenced
    scored_opt = scored & is_opt

    # A proven optimum cannot be beaten by an admissible solution.
    bad = vbs.loc[scored_opt & (vbs["gap_[%]"] < -GAP_TOL)]
    if len(bad):
        print(
            f"[WARN] Kris: {len(bad)} feasible solutions beat a proven optimum "
            f"(impossible) — check the objective or the setup convention:\n"
            f"       {bad['instance_name'].tolist()[:10]}"
        )

    infeasible_relaxation = vbs.loc[~feasible, "relaxation"].dropna()

    return pd.DataFrame([{
        "instance_set": "Kris",
        "objective": "total time",
        "n_instances": n_evaluated,
        "n_referenced": int(referenced.sum()),
        "n_proven_opt": int((referenced & is_opt).sum()),
        "n_feasible": int(feasible.sum()),
        "gap_to_ref_[%]": vbs.loc[scored, "gap_[%]"].mean() if scored.any() else np.nan,
        "gap_to_opt_[%]": vbs.loc[scored_opt, "gap_[%]"].mean() if scored_opt.any() else np.nan,
        "median_relaxation": infeasible_relaxation.median() if len(infeasible_relaxation) else np.nan,
        "max_relaxation": infeasible_relaxation.max() if len(infeasible_relaxation) else np.nan,
    }])


HEADER_MAP = {
    "instance_set": "Instance Set",
    "objective": "Objective",
    "n_instances": r"\# Instances",
    "n_referenced": r"\# Referenced",
    "n_proven_opt": r"\# Proven Opt.",
    "n_feasible": r"\# Feasible",
    "gap_to_ref_[%]": r"Gap to Ref. [\%]",
    "gap_to_opt_[%]": r"Gap to Opt. [\%]",
    "median_relaxation": "Median Relax.",
    "max_relaxation": "Max Relax.",
}


def run_kris_validation(
    df: pd.DataFrame,
    small_dir: Path,
    large_dir: Path,
    published_dir: Path,
    n_evaluated: int,
) -> pd.DataFrame | None:
    """Build, print and write the separate Kris validation table."""
    reference = build_kris_reference(small_dir, large_dir, published_dir)

    result = build_kris_vbs(df, reference)
    if result is None:
        return None
    vbs, kris = result

    scheduler_table = build_scheduler_feasibility(kris)
    print("\n" + "=" * 80)
    print("Kris: deadline feasibility by scheduler (total time is scheduler-independent)")
    print(scheduler_table.to_string(index=False))
    print("=" * 80)
    scheduler_table.to_csv("tab_kris_scheduler_feasibility.csv", index=False)
    scheduler_table.to_latex(
        "tab_kris_scheduler_feasibility.tex",
        index=False,
        escape=False,
        na_rep="--",
        column_format="lrrr",
        float_format="%.1f",
    )

    table = build_kris_table(vbs, n_evaluated)

    float_cols = [
        "gap_to_ref_[%]", "gap_to_opt_[%]", "median_relaxation", "max_relaxation",
    ]
    table[float_cols] = table[float_cols].round(3)

    print("\n" + "=" * 80)
    print("Kris (JOBPRSP-D): total time, deadline-feasible solutions only")
    print(table.to_string(index=False))
    print("=" * 80 + "\n")

    table.to_csv("tab_kris_validation.csv", index=False)

    tex = table.copy()
    tex["instance_set"] = tex["instance_set"].apply(lambda x: rf"\textit{{{x}}}")
    tex = tex.rename(columns=HEADER_MAP)
    tex.to_latex(
        "tab_kris_validation.tex",
        index=False,
        escape=False,
        na_rep="--",
        column_format="llrrrrrrrr",
        float_format="%.3f",
    )

    return vbs