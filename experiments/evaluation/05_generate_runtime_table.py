"""Generate the runtime table."""

from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[2]
EVAL_DIR = Path(__file__).resolve().parent
RUNTIME_DIR = EVAL_DIR / "runtimes"
TABLES = EVAL_DIR / "tables"

FILES = {
    "SPRP": ["SPRP.json"],
    "SPRP-SS": ["SPRP-SS.json"],
    "BahceciOencan": ["BahceciOencan.json"],
    "HennWaescher": [
        "HennWaescherUniform.json",
        "HennWaescherClassBased.json",
    ],
    "MuterOencan": ["MuterOencanWG.json"],
    "Foodmart": ["FoodmartData.json"],
    "Kris": ["KrisSmallDataCorrected.json", "KrisLargeData.json"],
}

def load_runtime_data() -> dict[str, dict]:
    merged: dict[str, dict] = {}

    for benchmark, filenames in FILES.items():
        combined: dict = {}
        for filename in filenames:
            path = RUNTIME_DIR / filename
            if not path.exists():
                raise FileNotFoundError(f"Missing raw runtime file: {path}")
            with path.open(encoding="utf-8") as stream:
                data = json.load(stream)
            for instance_name, values in data.items():
                key = f"{filename}:{instance_name}"
                combined[key] = values
        merged[benchmark] = combined

    return merged


def generate_table(data: dict[str, dict]) -> pd.DataFrame:
    rows: list[dict] = []
    for benchmark in FILES:
        instances = data[benchmark]
        build = np.asarray(
            [float(values["build_pipelines"]) for values in instances.values()]
        )
        execute = np.asarray(
            [float(values["run_pipelines"]) for values in instances.values()]
        )
        rows.append(
            {
                "Instance Set": benchmark,
                "n": len(instances),
                "synthesis_mean": build.mean(),
                "synthesis_std": build.std(),
                "execution_mean": execute.mean(),
                "execution_std": execute.std(),
            }
        )
    return pd.DataFrame(rows)


def write_outputs(table: pd.DataFrame) -> None:
    TABLES.mkdir(parents=True, exist_ok=True)
    table.to_csv(TABLES / "runtimes.csv", index=False)

    lines = [
        r"\begin{table}[tbp]",
        r"\centering",
        r"\caption{Per-instance runtimes for pipeline synthesis and portfolio execution (mean $\pm$ standard deviation, in seconds).}",
        r"\label{tab:runtimes}",
        r"\small",
        r"\begin{tabular}{@{}lrrr@{}}",
        r"\toprule",
        "Instance Set & $n$ & Pipeline Synthesis & Execution \\\\",
        r"\midrule",
    ]
    for _, row in table.iterrows():
        lines.append(
            rf"\textit{{{row['Instance Set']}}} & {int(row['n']):,} & "
            rf"${row['synthesis_mean']:.3f} \pm {row['synthesis_std']:.3f}$ & "
            rf"${row['execution_mean']:.3f} \pm {row['execution_std']:.3f}$ \\\\"
        )
    lines += [r"\bottomrule", r"\end{tabular}", r"\end{table}", ""]
    (TABLES / "runtimes.tex").write_text("\n".join(lines), encoding="utf-8")


def main() -> None:
    data = load_runtime_data()
    table = generate_table(data)
    write_outputs(table)
    print(table.to_string(index=False))
    print(f"\nRuntime table written to {TABLES}")


if __name__ == "__main__":
    main()
