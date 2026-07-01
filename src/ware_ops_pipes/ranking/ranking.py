from __future__ import annotations

import math
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Dict

import pandas as pd

from ware_ops_pipes.pipelines.io_helpers import load_json


class Metric(ABC):
    name: str
    direction: str = "min"

    @abstractmethod
    def compute(self, summary: Dict) -> float:
        ...


class DistanceMetric(Metric):
    name = "distance"
    direction = "min"

    def compute(self, summary: Dict) -> float:
        return float(summary["tours_summary"]["total_distance"])


class MakespanMetric(Metric):
    name = "makespan"
    direction = "min"

    def compute(self, summary: Dict) -> float:
        if "makespan" in summary:
            return float(summary["makespan"])
        return math.inf


class TardinessMetric(Metric):
    name = "tardiness"
    direction = "min"

    def compute(self, summary: Dict) -> float:
        if "avg_tardiness" in summary:
            return float(summary["avg_tardiness"])
        return math.inf


OBJECTIVE_TO_METRIC: dict[str, type[Metric]] = {
    "distance": DistanceMetric,
    "makespan": MakespanMetric,
    "completion_time": MakespanMetric,
    "tardiness": TardinessMetric,
}


class RankingEvaluator:
    """
    Evaluates pipeline summaries according to the objective declared in the data card.
    """

    def __init__(
        self,
        output_dir: str,
        instance_name: str,
        data_card: Any,
        taxonomy: dict,
    ):
        self.output_dir = Path(output_dir)
        self.instance_name = instance_name
        self.data_card = data_card
        self.taxonomy = taxonomy
        self.df_result: pd.DataFrame | None = None

        self.problem_class = self._data_card_value("problem_class")
        self.objective = self._data_card_value("objective")

        self._validate_objective()
        self.metric = self._resolve_metric()

    def evaluate(self) -> pd.DataFrame:
        results = []

        summary_files = sorted(self.output_dir.rglob("summary.json"))

        for file in summary_files:
            summary = load_json(str(file))
            value = self.metric.compute(summary)

            pipeline_id = self._pipeline_id(summary)

            results.append(
                {
                    "pipeline_id": pipeline_id,
                    "problem_class": self.problem_class,
                    "objective": self.objective,
                    "metric": self.metric.name,
                    "item_assignment_algo": summary.get("item_assignment_algo"),
                    "batching_algo": summary.get("batching_algo"),
                    "routing_algo": summary.get("routing_algo"),
                    "scheduling_algo": summary.get("scheduling_algo"),
                    "value": value,
                    "summary_path": str(file.relative_to(self.output_dir)),
                }
            )

        if not results:
            print(f"No results found in {self.output_dir}")
            return pd.DataFrame()

        df = pd.DataFrame(results)
        df = df.sort_values(
            "value",
            ascending=self.metric.direction == "min",
        ).reset_index(drop=True)

        df["rank"] = range(1, len(df) + 1)

        best = df.iloc[0]["value"]
        df["gap_to_best"] = df["value"] - best
        df["gap_pct"] = ((df["value"] - best) / best * 100.0) if best != 0 else 0.0

        output_file = self.output_dir / f"ranking_{self.objective}.csv"
        df.to_csv(output_file, index=False)

        print(f"\nTop 5 pipelines for {self.instance_name}:")
        print(
            df[["rank", "pipeline_id", "objective", "value", "gap_pct"]]
            .head()
            .to_string(index=False)
        )
        print(f"\nSaved: {output_file}\n")

        self.df_result = df
        return df

    def _validate_objective(self) -> None:
        if self.problem_class not in self.taxonomy:
            raise ValueError(
                f"Unknown problem_class {self.problem_class!r}. "
                f"Available: {list(self.taxonomy)}"
            )

        allowed = self.taxonomy[self.problem_class]["objectives"]

        if self.objective not in allowed:
            raise ValueError(
                f"Objective {self.objective!r} is not allowed for "
                f"problem_class {self.problem_class!r}. Allowed: {allowed}"
            )

    def _resolve_metric(self) -> Metric:
        if self.objective not in OBJECTIVE_TO_METRIC:
            raise ValueError(
                f"No metric registered for objective {self.objective!r}. "
                f"Registered: {list(OBJECTIVE_TO_METRIC)}"
            )

        return OBJECTIVE_TO_METRIC[self.objective]()

    def _data_card_value(self, key: str):
        if isinstance(self.data_card, dict):
            return self.data_card[key]

        return getattr(self.data_card, key)

    @staticmethod
    def _pipeline_id(summary: Dict) -> str:
        stages = [
            summary.get("item_assignment_algo"),
            summary.get("batching_algo"),
            summary.get("routing_algo"),
            summary.get("scheduling_algo"),
        ]

        return "+".join(stage for stage in stages if stage is not None)