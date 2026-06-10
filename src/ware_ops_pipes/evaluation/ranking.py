from __future__ import annotations

import pickle
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Dict

import pandas as pd

from ware_ops_pipes.utils.io_helpers import load_json


class RankingEvaluator(ABC):
    """
    Class to evaluate output of a Pipeline Runner.
    Evaluates and outputs a batch of pipelines.
    """
    def __init__(self, output_dir: str, instance_name: str):
        self.output_dir = Path(output_dir)
        self.instance_name = instance_name

    @abstractmethod
    def evaluate(self):
        pass


class RankingEvaluatorDistance(RankingEvaluator):
    def __init__(self, output_dir: str, instance_name: str):

        super().__init__(output_dir, instance_name)
        self.df_result = None

    def evaluate(self, metric_path: str = "tours_summary.total_distance",
                 minimize: bool = True) -> pd.DataFrame:
        """
        Rank pipelines by metric.

        Args:
            metric_path: Metric to rank by (e.g., 'tours_summary.total_distance')
            minimize: True if lower is better

        Returns:
            DataFrame with ranked results
        """
        # Collect all summaries
        results = []
        for file in self.output_dir.glob("*summary.json"):
            summary = load_json(str(file))

            # Extract metric
            metric_value = self._get_metric(summary, metric_path)
            if metric_value is None:
                continue

            # Extract pipeline info
            pipeline_id = f"{summary.get('item_assignment_algo')}+{summary.get('batching_algo')}+{summary.get('routing_algo')}"

            results.append({
                "pipeline_id": pipeline_id,
                "item_assignment_algo": summary.get('item_assignment_algo'),
                "batching_algo": summary.get('batching_algo'),
                "routing_algo": summary.get('routing_algo'),
                "value": metric_value,
            })

        if not results:
            print(f"No results found in {self.output_dir}")
            return pd.DataFrame()

        # Sort and rank
        df = pd.DataFrame(results)
        df = df.sort_values("value", ascending=minimize).reset_index(drop=True)
        df['rank'] = range(1, len(df) + 1)

        # Calculate gap to best
        best = df.iloc[0]['value']
        df['gap_to_best'] = df['value'] - best
        df['gap_pct'] = ((df['value'] - best) / best * 100.0) if best != 0 else 0

        # Save
        output_file = self.output_dir / f"ranking_{metric_path.replace('.', '_')}.csv"
        df.to_csv(output_file, index=False)

        # Print top 5
        print(f"\nTop 5 pipelines for {self.instance_name}:")
        print(df[['rank', 'pipeline_id', 'value', 'gap_pct']].head().to_string(index=False))
        print(f"\nSaved: {output_file}\n")

        self.df_result = df
        return df

    def _get_metric(self, summary: Dict, metric_path: str):
        """Extract metric using dot notation"""
        if '.' in metric_path:
            parts = metric_path.split('.')
            value = summary
            for part in parts:
                if isinstance(value, dict) and part in value:
                    value = value[part]
                else:
                    return None
            return value
        return summary.get(metric_path)


class RankingEvaluatorSequencing(RankingEvaluator):
    def __init__(self, output_dir: str, instance_name: str):

        super().__init__(output_dir, instance_name)

    @staticmethod
    def load_sequencing_solutions(base_dir: str):
        sol_files = Path(base_dir).glob("**/*scheduling_plan.pkl")
        solutions = {}
        for f in sol_files:
            with open(f, "rb") as fh:
                try:
                    solutions[f.name] = pickle.load(fh)
                except Exception as e:
                    print(f"❌ Failed to load {f}: {e}")
        return solutions

    def evaluate(self, metric_path: str = "tours_summary.due_dates",
                 minimize: bool = True) -> pd.DataFrame:
        """
        Rank pipelines by metric.

        Args:
            metric_path: Metric to rank by (e.g., 'tours_summary.total_distance')
            minimize: True if lower is better

        Returns:
            DataFrame with ranked results
        """
        # Collect all summaries
        solutions = self.load_sequencing_solutions(str(self.output_dir))

        best_key, best_dist = None, float("inf")
        for k, plan in solutions.items():
            dist = sum(a.distance for a in plan.sequencing_solutions.jobs)
            if dist < best_dist:
                best_key, best_dist = k, dist
        print(best_key)
        solution = solutions[best_key].sequencing_solutions
        # df = self._evaluate_due_dates(solution)
        return solution

    # def _evaluate_due_dates(self, assignments: list[Job]):
    #     # order_by_id = {o.order_id: o for o in orders}
    #     records = []
    #     for ass in assignments:
    #         end_time = ass.end_time
    #         for on in ass.route.pick_list.order_numbers:
    #             # o = order_by_id.get(on)
    #             # if o is None:
    #             #     continue
    #             # if o.due_date is None:
    #             #     continue  # skip if no due date
    #
    #             arrival_time = o.order_date
    #             start_time = ass.start_time
    #             due_ts = o.due_date  # .timestamp()
    #             lateness = end_time - due_ts
    #             records.append({
    #                 "order_number": on,
    #                 "arrival_time": arrival_time,
    #                 "start_time": start_time,
    #                 "picker_id": ass.picker_id,
    #                 "completion_time": end_time,
    #                 "due_date": o.due_date,
    #                 "lateness": lateness,
    #                 "tardiness": max(0, lateness),
    #                 "on_time": end_time <= due_ts,
    #             })
    #     return pd.DataFrame(records)

    def _get_metric(self, summary: Dict, metric_path: str):
        """Extract metric using dot notation"""
        if '.' in metric_path:
            parts = metric_path.split('.')
            value = summary
            for part in parts:
                if isinstance(value, dict) and part in value:
                    value = value[part]
                else:
                    return None
            return value
        return summary.get(metric_path)
