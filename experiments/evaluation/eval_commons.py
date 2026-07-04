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

    for data in summary_data:
        row = {
            "instance_name": data.get("instance_name", None),
            "instance_set": data.get("instance_set", None),
            "total_distance": data.get("tours_summary", {}).get("total_distance", 0),
            "makespan": data.get("makespan", None),
            "on_time_rate": data.get("on_time_rate", None),
            "avg_tardiness": data.get("avg_tardiness", None),
            "max_lateness": data.get("max_lateness", None),
            "max_tardiness": data.get("max_tardiness", None),
            "avg_lateness": data.get("avg_lateness", None),
        }

        # Algo names and times from provenance
        stage_to_field = {
            "item_assignment": "item_assignment_algo",
            "batching": "batching_algo",
            "routing": "routing_algo",
            "scheduling": "scheduling_algo",
        }
        provenance = data.get("provenance", [])
        prov_lookup = {e["stage"]: e for e in provenance if "stage" in e}

        for stage, field in stage_to_field.items():
            entry = prov_lookup.get(stage)
            row[field] = entry["task_class"] if entry and "task_class" in entry else data.get(field, None)

        batching_entry = prov_lookup.get("batching")
        row["routing_input_time"] = batching_entry["time"] if batching_entry and "time" in batching_entry else data.get(
            "tours_summary", {}).get("routing_input_time", 0)

        # Per-tour route time (only available when routing is separate from batching)
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
            routing_entry = prov_lookup.get("routing")
            row["total_route_time"] = routing_entry["time"] if routing_entry and "time" in routing_entry else data.get("tours_summary", {}).get("execution_time", 0)

        # Batch distance statistics
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


def _collect_paths_by_set(base_path: str, sets_to_load: list[str]) -> dict[str, list[Path]]:
    base = Path(base_path)
    by_set: dict[str, list[Path]] = {}
    for s in sets_to_load:
        inst_set_dir = base / s
        print(inst_set_dir)
        if not inst_set_dir.is_dir():
            continue
        paths = []
        for inst_dir in inst_set_dir.iterdir():
            if inst_dir.is_dir():
                for p in inst_dir.iterdir():
                    if p.is_file() and p.name.endswith("summary.json"):
                        paths.append(p)
        if paths:
            by_set[s] = paths
    return by_set


def _load_one(path: Path) -> dict | None:
    try:
        data = orjson.loads(path.read_bytes())
        data["file_path"] = str(path)
        return data
    except Exception as e:
        print(f"Error loading {path}: {e}")
        return None


def load_summary_jsons_fast(base_path: str, sets_to_load: list[str]) -> list[dict]:
    by_set = _collect_paths_by_set(base_path, sets_to_load)
    all_data: list[dict] = []
    total_sets = len(by_set)
    done_sets = 0
    print(all_data[:5])

    for s, paths in by_set.items():
        ok = 0
        errs = 0
        desc = f"{s} ({len(paths)} files)"
        if _USE_TQDM:
            pbar = tqdm(total=len(paths), desc=desc, leave=False)
        # File-level parallelism per set
        with ThreadPoolExecutor(max_workers=os.cpu_count() or 8) as ex:
            futures = {ex.submit(_load_one, p): p for p in paths}
            for fut in as_completed(futures):
                res = fut.result()
                if res is None:
                    errs += 1
                else:
                    ok += 1
                    all_data.append(res)
                if _USE_TQDM:
                    pbar.update(1)
        if _USE_TQDM:
            pbar.close()

        done_sets += 1
        print(f"[{done_sets}/{total_sets}] Finished {s}: {ok} ok, {errs} errors")

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