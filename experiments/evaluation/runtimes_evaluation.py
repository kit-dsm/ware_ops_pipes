#!/usr/bin/env python3
"""Analyze pipeline generation runtimes and produce LaTeX tables."""

import json
from pathlib import Path
import numpy as np

RUNTIME_DIR = Path("../output/runtimes")
FILES = ["BahceciOencan.json", "FoodmartData.json", "IOPVRP.json", "SPRP-SS.json"]
PHASES = ["load_domain", "filter_and_import", "build_pipelines", "run_pipelines", "total"]


def load_data():
    """Load all JSON files and return dict of {dataset: {instance: timings}}."""
    data = {}
    for f in FILES:
        name = f.replace(".json", "")
        with open(RUNTIME_DIR / f) as fp:
            data[name] = json.load(fp)
    return data


def compute_stats(values):
    """Return mean, std, min, max for a list of values."""
    arr = np.array(values)
    return {
        "mean": np.mean(arr),
        "std": np.std(arr),
        "min": np.min(arr),
        "max": np.max(arr),
        "n": len(arr),
    }


def analyze(data):
    """Compute per-dataset and overall statistics."""
    results = {}
    for dataset, instances in data.items():
        results[dataset] = {"n": len(instances)}
        for phase in PHASES:
            values = [inst[phase] for inst in instances.values()]
            results[dataset][phase] = compute_stats(values)
    return results


def to_latex_table(results):
    """Generate LaTeX table with runtime statistics."""
    lines = []
    lines.append(r"\begin{table}[ht]")
    lines.append(r"\centering")
    lines.append(r"\caption{Pipeline generation runtimes (seconds)}")
    lines.append(r"\label{tab:runtimes}")
    lines.append(r"\begin{tabular}{l r r r r r}")
    lines.append(r"\toprule")
    lines.append(r"Dataset & $n$ & Load & Build & Run & Total \\")
    lines.append(r"\midrule")

    for dataset, stats in results.items():
        n = stats["n"]
        load = stats["load_domain"]["mean"]
        build = stats["build_pipelines"]["mean"]
        run = stats["run_pipelines"]["mean"]
        total = stats["total"]["mean"]
        lines.append(f"{dataset} & {n} & {load:.4f} & {build:.3f} & {run:.3f} & {total:.3f} \\\\")

    lines.append(r"\bottomrule")
    lines.append(r"\end{tabular}")
    lines.append(r"\end{table}")
    return "\n".join(lines)


def to_latex_detailed(results):
    """Generate detailed LaTeX table with mean ± std."""
    lines = []
    lines.append(r"\begin{table}[ht]")
    lines.append(r"\centering")
    lines.append(r"\caption{Detailed pipeline generation runtimes (mean $\pm$ std, in seconds)}")
    lines.append(r"\label{tab:runtimes-detailed}")
    lines.append(r"\begin{tabular}{l r r r r}")
    lines.append(r"\toprule")
    lines.append(r"Dataset & Load Domain & Build Pipelines & Run Pipelines & Total \\")
    lines.append(r"\midrule")

    for dataset, stats in results.items():
        load = stats["load_domain"]
        build = stats["build_pipelines"]
        run = stats["run_pipelines"]
        total = stats["total"]
        lines.append(
            f"{dataset} & "
            f"${load['mean']:.4f} \\pm {load['std']:.4f}$ & "
            f"${build['mean']:.3f} \\pm {build['std']:.3f}$ & "
            f"${run['mean']:.3f} \\pm {run['std']:.3f}$ & "
            f"${total['mean']:.3f} \\pm {total['std']:.3f}$ \\\\"
        )

    lines.append(r"\bottomrule")
    lines.append(r"\end{tabular}")
    lines.append(r"\end{table}")
    return "\n".join(lines)


def main():
    data = load_data()
    results = analyze(data)

    print("=" * 60)
    print("SUMMARY")
    print("=" * 60)
    for dataset, stats in results.items():
        print(f"\n{dataset} (n={stats['n']})")
        for phase in PHASES:
            s = stats[phase]
            print(f"  {phase:20s}: {s['mean']:.4f} ± {s['std']:.4f} (min={s['min']:.4f}, max={s['max']:.4f})")

    print("\n" + "=" * 60)
    print("LATEX TABLE (simple)")
    print("=" * 60)
    print(to_latex_table(results))

    print("\n" + "=" * 60)
    print("LATEX TABLE (detailed)")
    print("=" * 60)
    print(to_latex_detailed(results))


if __name__ == "__main__":
    main()