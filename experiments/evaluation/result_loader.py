"""Load experiment summary files."""

import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import numpy as np
import orjson
import pandas as pd
from tqdm.auto import tqdm


def _find_summary_files(base_path: str, instance_sets: list[str]) -> dict[str, list[Path]]:
    base = Path(base_path)
    files = {}
    for instance_set in instance_sets:
        directory = base / instance_set
        if not directory.is_dir():
            continue
        paths = sorted(directory.glob("*/*summary.json"))
        if paths:
            files[instance_set] = paths
    return files


def _read_summary(path: Path) -> dict | None:
    try:
        data = orjson.loads(path.read_bytes())
        data["file_path"] = str(path)
        return data
    except (OSError, orjson.JSONDecodeError) as error:
        print(f"Could not read {path}: {error}")
        return None


def load_summary_jsons(base_path: str, instance_sets: list[str]) -> list[dict]:
    files = _find_summary_files(base_path, instance_sets)
    summaries = []

    for instance_set, paths in files.items():
        errors = 0
        with ThreadPoolExecutor(max_workers=os.cpu_count() or 8) as executor:
            futures = [executor.submit(_read_summary, path) for path in paths]
            for future in tqdm(
                as_completed(futures),
                total=len(futures),
                desc=instance_set,
                leave=False,
            ):
                summary = future.result()
                if summary is None:
                    errors += 1
                else:
                    summaries.append(summary)
        print(f"{instance_set}: {len(paths) - errors} files loaded, {errors} errors")

    return summaries


def create_summary_dataframe(summaries: list[dict]) -> pd.DataFrame:
    rows = []

    for summary in summaries:
        tours = summary.get("tours_summary", {})
        row = {
            "instance_name": summary.get("instance_name"),
            "instance_set": summary.get("instance_set"),
            "total_distance": tours.get("total_distance", 0),
            "makespan": summary.get("makespan"),
            "on_time_rate": summary.get("on_time_rate"),
            "avg_tardiness": summary.get("avg_tardiness"),
            "max_lateness": summary.get("max_lateness"),
            "max_tardiness": summary.get("max_tardiness"),
            "avg_lateness": summary.get("avg_lateness"),
            "total_time": summary.get("total_time"),
            "n_routes": summary.get("n_routes"),
            "max_stretch": summary.get("max_stretch"),
        }

        stages = {
            entry["stage"]: entry
            for entry in summary.get("provenance", [])
            if "stage" in entry
        }
        for stage, column in {
            "item_assignment": "item_assignment_algo",
            "batching": "batching_algo",
            "routing": "routing_algo",
            "scheduling": "scheduling_algo",
        }.items():
            entry = stages.get(stage, {})
            row[column] = entry.get("task_class", summary.get(column))

        row["ia_time"] = stages.get("item_assignment", {}).get("time", 0.0)
        row["routing_input_time"] = stages.get("batching", {}).get(
            "time", tours.get("routing_input_time", 0)
        )
        row["scheduling_time"] = stages.get("scheduling", {}).get("time", 0.0)

        route_times = list(tours.get("time_per_tour", {}).values())
        if route_times:
            row.update(
                {
                    "total_route_time": sum(route_times),
                    "min_route_time": min(route_times),
                    "max_route_time": max(route_times),
                    "avg_route_time": np.mean(route_times),
                    "median_route_time": np.median(route_times),
                    "std_route_time": np.std(route_times),
                }
            )
        else:
            row["total_route_time"] = stages.get("routing", {}).get(
                "time", tours.get("execution_time", 0)
            )

        features = summary.get("instance_features", {})
        for name in [
            "n_orders",
            "n_pick_locations",
            "n_aisles",
            "n_blocks",
            "n_resources",
            "storage_type",
            "n_order_lines",
        ]:
            row[name] = features.get(name)

        loader_timing = summary.get("loader_timing", {})
        for name in [
            "layout_parse_time",
            "layout_build_time",
            "layout_load_time",
            "layout_cache_hit",
            "instance_parse_time",
            "instance_build_time",
            "instance_load_time",
            "instance_cache_hit",
        ]:
            row[name] = loader_timing.get(name)

        distances = list(tours.get("tour_distances", {}).values())
        if distances:
            row.update(
                {
                    "num_batches": len(distances),
                    "min_batch_distance": min(distances),
                    "max_batch_distance": max(distances),
                    "avg_batch_distance": np.mean(distances),
                    "median_batch_distance": np.median(distances),
                    "std_batch_distance": np.std(distances),
                }
            )

        rows.append(row)

    return pd.DataFrame(rows)
