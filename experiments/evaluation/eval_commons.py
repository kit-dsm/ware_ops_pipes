import os
import re
from pathlib import Path
import json
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from typing import Dict, List
from concurrent.futures import ThreadPoolExecutor, as_completed
import orjson
from tqdm.auto import tqdm
_USE_TQDM = True

def _path_metadata(path: Path, base: Path) -> dict:
    rel = path.relative_to(base)
    parts = rel.parts

    meta = {
        "_path_instance_set": None,
        "_path_instance_name": None,
        "_path_pipeline_chain_fingerprint": None,
        "_path_result_aggregation": None,
    }

    # Expected:
    # <instance_set>/<instance_name>/<chain_fp>/<result_task>/summary.json
    if len(parts) >= 5 and parts[-1] == "summary.json":
        meta["_path_instance_set"] = parts[0]
        meta["_path_instance_name"] = parts[1]
        meta["_path_pipeline_chain_fingerprint"] = parts[2]
        meta["_path_result_aggregation"] = parts[3]

    return meta


def _collect_paths_by_set(base_path: str, sets_to_load: list[str]) -> dict[str, list[Path]]:
    base = Path(base_path)
    by_set: dict[str, list[Path]] = {}

    for instance_set in sets_to_load:
        inst_set_dir = base / instance_set
        print(f"Searching: {inst_set_dir.resolve()}")

        if not inst_set_dir.is_dir():
            continue

        paths = sorted(inst_set_dir.rglob("summary.json"))

        if paths:
            by_set[instance_set] = paths

    return by_set

def load_summary_jsons(base_path: str, sets_to_load: list[str]) -> List[Dict]:
    """
    Load all summary JSON files from the specified directory structure.
    """
    summary_data = []

    # Walk through all directories
    for instance_set in os.listdir(base_path):
        if instance_set not in sets_to_load:  # , "BahceciOencan"
            continue
        instance_set_path = os.path.join(base_path, instance_set)

        # Skip if not a directory
        if not os.path.isdir(instance_set_path):
            continue

        for inst in os.listdir(instance_set_path):
            inst_path = os.path.join(instance_set_path, inst)

            # Skip if not a directory
            if not os.path.isdir(inst_path):
                continue

            for content in os.listdir(inst_path):
                # Filter only files that end with "summary.json"
                if content.endswith("summary.json"):
                    file_path = os.path.join(inst_path, content)
                    # print(f"Loading: {file_path}")

                    try:
                        # Load the JSON file
                        with open(file_path, "r") as f:
                            data = json.load(f)

                        # Add file path info
                        data["file_path"] = file_path
                        summary_data.append(data)
                    except Exception as e:
                        print(f"Error loading {file_path}: {e}")

    return summary_data


def create_summary_dataframe(summary_data: List[Dict]) -> pd.DataFrame:
    rows = []

    stage_to_field = {
        "item_assignment": "item_assignment_algo",
        "batching": "batching_algo",
        "routing": "routing_algo",
        "scheduling": "scheduling_algo",
    }

    for data in summary_data:
        row = {
            "file_path": data.get("file_path"),
            "instance_name": data.get("instance_name") or data.get("_path_instance_name"),
            "instance_set": data.get("instance_set") or data.get("_path_instance_set"),
            "pipeline_chain_fingerprint": (
                data.get("pipeline_chain_fingerprint")
                or data.get("_path_pipeline_chain_fingerprint")
            ),
            "result_aggregation": (
                data.get("result_aggregation")
                or data.get("_path_result_aggregation")
            ),
            "total_distance": data.get("tours_summary", {}).get("total_distance", 0),
            "makespan": data.get("makespan", None),
            "on_time_rate": data.get("on_time_rate", None),
            "avg_tardiness": data.get("avg_tardiness", None),
            "max_lateness": data.get("max_lateness", None),
            "max_tardiness": data.get("max_tardiness", None),
            "avg_lateness": data.get("avg_lateness", None),
        }

        provenance = data.get("provenance", [])
        prov_lookup = {
            e["stage"]: e
            for e in provenance
            if isinstance(e, dict) and "stage" in e
        }

        for stage, algo_col in stage_to_field.items():
            entry = prov_lookup.get(stage, {})

            row[algo_col] = (
                    entry.get("algo")
                    or data.get(algo_col)
                    or entry.get("task_class")
            )
            row[f"{stage}_task_class"] = entry.get("task_class")
            row[f"{stage}_time"] = entry.get("time", data.get(f"{stage}_time"))

            row[f"{stage}_algo_fingerprint"] = entry.get(
                "algo_fingerprint",
                data.get(f"{stage}_algo_fingerprint"),
            )
            row[f"{stage}_own_fingerprint"] = entry.get(
                "own_fingerprint",
                data.get(f"{stage}_own_fingerprint"),
            )
            row[f"{stage}_chain_fingerprint"] = entry.get(
                "chain_fingerprint",
                data.get(f"{stage}_chain_fingerprint"),
            )
            row[f"{stage}_config"] = entry.get(
                "config",
                data.get(f"{stage}_config"),
            )
            row[f"{stage}_target_path"] = entry.get("target_path")

        # Keep old compatibility columns.
        batching_entry = prov_lookup.get("batching", {})
        row["routing_input_time"] = batching_entry.get(
            "time",
            data.get("tours_summary", {}).get("routing_input_time", 0),
        )

        batch_times = data.get("tours_summary", {}).get("time_per_tour", {})
        if batch_times:
            times = list(batch_times.values())
            row["total_route_time"] = sum(times)
            row["min_route_time"] = min(times)
            row["max_route_time"] = max(times)
            row["avg_route_time"] = sum(times) / len(times)
            row["median_route_time"] = np.median(times)
            row["std_route_time"] = np.std(times)
        else:
            routing_entry = prov_lookup.get("routing", {})
            row["total_route_time"] = routing_entry.get(
                "time",
                data.get("tours_summary", {}).get("execution_time", 0),
            )

        batch_distances = data.get("tours_summary", {}).get("tour_distances", {})
        if batch_distances:
            distances = list(batch_distances.values())
            row.update({
                "num_batches": len(distances),
                "min_batch_distance": min(distances),
                "max_batch_distance": max(distances),
                "avg_batch_distance": sum(distances) / len(distances),
                "median_batch_distance": np.median(distances),
                "std_batch_distance": np.std(distances),
            })

        rows.append(row)

    return pd.DataFrame(rows)


def _load_one(path: Path, base: Path) -> dict | None:
    try:
        data = orjson.loads(path.read_bytes())
        data["file_path"] = str(path)
        data.update(_path_metadata(path, base))
        return data
    except Exception as e:
        print(f"Error loading {path}: {e}")
        return None


def load_summary_jsons_fast(base_path: str, sets_to_load: list[str]) -> list[dict]:
    base = Path(base_path)
    by_set = _collect_paths_by_set(base_path, sets_to_load)

    all_data: list[dict] = []
    total_sets = len(by_set)

    for done_sets, (instance_set, paths) in enumerate(by_set.items(), start=1):
        ok = 0
        errs = 0
        desc = f"{instance_set} ({len(paths)} files)"

        pbar = tqdm(total=len(paths), desc=desc, leave=False) if _USE_TQDM else None

        with ThreadPoolExecutor(max_workers=os.cpu_count() or 8) as ex:
            futures = {ex.submit(_load_one, p, base): p for p in paths}

            for fut in as_completed(futures):
                res = fut.result()
                if res is None:
                    errs += 1
                else:
                    ok += 1
                    all_data.append(res)

                if pbar is not None:
                    pbar.update(1)

        if pbar is not None:
            pbar.close()

        print(f"[{done_sets}/{total_sets}] Finished {instance_set}: {ok} ok, {errs} errors")

    return all_data


def build_strategy(row):
    cols = ["item_assignment_algo", "batching_algo", "routing_algo", "scheduling_algo"]
    parts = [str(row[c]) for c in cols if pd.notna(row[c]) and row[c] != ""]
    return "+".join(parts)


def vbs_analysis(df, metric_col="total_distance", strategy_col="strategy", instance_col="instance_name", min_max="min"):
    """
    Performs VBS vs SBS analysis.
    Returns a DataFrame with SBS mean, VBS mean, mean regret, and relative gain.
    """
    results = []
    # VBS per instance
    if min_max == "min":
        vbs_per_instance = df.groupby(instance_col)[metric_col].min()
    else:
        vbs_per_instance = df.groupby(instance_col)[metric_col].max()
    vbs_strategies = df.loc[df.groupby("instance_name")[metric_col].idxmin(), ["instance_name", "strategy"]]
    vbs_mean = vbs_per_instance.mean()

    # SBS (best on average in group)
    avg_by_strategy = df.groupby(strategy_col)[metric_col].mean()
    if min_max == "min":
        sbs_strategy = avg_by_strategy.idxmin()
        sbs_mean = avg_by_strategy.min()
    else:
        sbs_strategy = avg_by_strategy.idxmax()
        sbs_mean = avg_by_strategy.max()
    # SBS performance per instance
    sbs_perf_per_instance = df[df[strategy_col] == sbs_strategy].set_index(instance_col)[metric_col]
    winner_counts = vbs_strategies["strategy"].value_counts()
    if min_max == "min":
        # lower is better
        regret = sbs_perf_per_instance - vbs_per_instance
        rel_gain = 100 * (sbs_mean - vbs_mean) / sbs_mean
    else:
        # higher is better
        regret = vbs_per_instance - sbs_perf_per_instance
        rel_gain = 100 * (vbs_mean - sbs_mean) / sbs_mean

    results.append({
        "instance_set": df["instance_set"].unique()[0],
        "SBS Strategy": sbs_strategy,
        "SBS Mean": sbs_mean,
        "VBS Mean": vbs_mean,
        # "Mean Regret": mean_regret,
        "Relative Gain %": rel_gain
    })

    return pd.DataFrame(results), winner_counts


def grouped_vbs_analysis(df, group_col, metric_col="total_distance", strategy_col="strategy",
                         instance_col="instance_name"):
    """
    Performs VBS vs SBS analysis grouped by a category (e.g., storage policy).
    Returns a DataFrame with SBS mean, VBS mean, mean regret, and relative gain per group.
    """
    results = []
    winner_counts = 0
    for group, subdf in df.groupby(group_col):
        # VBS per instance
        vbs_per_instance = subdf.groupby(instance_col)[metric_col].min()
        vbs_mean = vbs_per_instance.mean()
        vbs_strategies = subdf.loc[
            subdf.groupby("instance_name")["total_distance"].idxmin(), ["instance_name", "strategy"]]

        # SBS (best on average in group)
        avg_by_strategy = subdf.groupby(strategy_col)[metric_col].mean()
        sbs_strategy = avg_by_strategy.idxmin()
        sbs_mean = avg_by_strategy.min()

        # SBS performance per instance
        sbs_perf_per_instance = subdf[subdf[strategy_col] == sbs_strategy].set_index(instance_col)[metric_col]

        # Regret
        regret = sbs_perf_per_instance - vbs_per_instance
        mean_regret = regret.mean()
        rel_gain = 100 * (sbs_mean - vbs_mean) / sbs_mean
        winner_counts = vbs_strategies["strategy"].value_counts()

        results.append({
            group_col: group,
            "SBS Strategy": sbs_strategy,
            "SBS Mean": sbs_mean,
            "VBS Mean": vbs_mean,
            # "Mean Regret": mean_regret,
            "Relative Gain %": rel_gain
        })

    return pd.DataFrame(results), winner_counts


def plot_winners(winner_counts: pd.DataFrame()):
    plt.figure(figsize=(8, 4))
    top_winners = winner_counts.head(10)
    sns.barplot(x=top_winners.values, y=top_winners.index, palette="Blues_r", hue=top_winners.index, legend=False)
    plt.xlabel("Number of Instances Won")
    plt.ylabel("Strategy")
    plt.title("Top Winning Strategies (VBS)")
    plt.tight_layout()
    plt.show()


def plot_winners_pareto(winner_counts: pd.DataFrame()):
    plt.figure(figsize=(8, 6))
    winner_percent = winner_counts / winner_counts.sum() * 100
    winner_percent.sort_values().plot(kind='barh', color='skyblue')
    plt.xlabel("Percentage of Instances Won (%)")
    plt.ylabel("Strategy")
    plt.title("Overall VBS Winner Distribution")
    plt.show()


def parse_solution_file(filepath):
    """Parse a single solution file into batch/order data."""
    text = Path(filepath).read_text()

    # Parse batch lines
    batches = []
    for m in re.finditer(
            r'PickerID\t(\d+)\tBatchID\t(\d+)\tPreviousBatch\t(\d+)\tNoOders\t(\d+)\tNoOderLines\t(\d+)\tBatchDistance\t(\d+)\tBatchComplTime\t(\d+)',
            text):
        batches.append({
            'picker_id': int(m[1]), 'batch_id': int(m[2]),
            'n_orders': int(m[4]), 'n_lines': int(m[5]),
            'distance': int(m[6]), 'completion_time': int(m[7]),
        })

    # Parse order lines
    orders = []
    for m in re.finditer(
            r'OrderID\t(\d+)\tNoOrderLines\t(\d+)\tNextOrderID\t(\d+)\tPickerID\t(\d+)\tBatchID\t(\d+)\tDueTime\t(\d+)\tCompletionTime\t(\d+)',
            text):
        orders.append({
            'order_id': int(m[1]), 'due_time': int(m[6]),
            'completion_time': int(m[7]),
        })

    # Aggregate
    total_distance = sum(b['distance'] for b in batches)
    makespan = max(b['completion_time'] for b in batches)
    tardiness = sum(max(0, o['completion_time'] - o['due_time']) for o in orders)
    max_tardiness = max((max(0, o['completion_time'] - o['due_time']) for o in orders), default=0)
    n_tardy = sum(1 for o in orders if o['completion_time'] > o['due_time'])
    n_on_time = sum(1 for o in orders if o['completion_time'] <= o['due_time'])
    on_time_rate = n_on_time / len(orders) * 100

    return {
        'best_total_distance': total_distance,
        'best_makespan': makespan,
        'best_tardiness': tardiness,
        'best_max_tardiness': max_tardiness,
        'best_on_time_rate': on_time_rate,
        'best_n_tardy': n_tardy,
        'best_n_batches': len(batches),
        'n_orders': len(orders),
        'n_pickers': len(set(b['picker_id'] for b in batches)),
    }


def parse_solution_dir(directory, instance_set="KrisSmallData", glob="*.txt"):
    """Parse all solution files in a directory into a DataFrame."""
    rows = []
    for fp in sorted(Path(directory).glob(glob)):
        split_name = fp.stem.split("_")
        row = parse_solution_file(fp)
        # row['instance_set_'] = instance_set
        row['instance_name'] = f"instances_{split_name[2]}_{split_name[3]}"
        rows.append(row)
    return pd.DataFrame(rows)