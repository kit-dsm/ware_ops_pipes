from __future__ import annotations

from luigi.task import flatten

from ware_ops_pipes.pipelines.io_helpers import load_pickle

_SOL_KEY_TO_STAGE = {
    "item_assignment_sol": "item_assignment",
    "batching_sol": "batching",
    "routing_sol": "routing",
    "sequencing_sol": "scheduling",
    "assignment_sol": "assignment",
    "scheduling_sol": "scheduling",
}


def collect_from_graph(task) -> dict[str, dict]:
    """Walk the task DAG depth-first and collect solutions + provenance.

    Returns a dict keyed by stage name::

        {
            "item_assignment": {
                "stage": "item_assignment",
                "task_class": "GreedyIA",
                "algo": "GreedyItemAssignment",
                "time": 0.003,
                "solution": <ItemAssignmentSolution>,
                "target_path": "/path/to/item_assignment_sol.pkl",
            },
            "batching": { ... },   # absent for CombinedBR path
            "routing":  { ... },
            ...
        }
    """
    result: dict[str, dict] = {}
    _collect_recursive(task, result, visited=set())
    return result


def _collect_recursive(task, result: dict, visited: set):
    task_id = id(task)
    if task_id in visited:
        return
    visited.add(task_id)

    # Recurse into dependencies first (depth-first)
    deps = task.requires()
    if isinstance(deps, dict):
        children = list(deps.values())
    elif isinstance(deps, (list, tuple)):
        children = list(deps)
    else:
        children = [deps]

    for dep in children:
        _collect_recursive(dep, result, visited)

    # Check if this task has a solution output we recognise
    if not hasattr(task, "output"):
        return

    outputs = task.output()
    for sol_key, stage_name in _SOL_KEY_TO_STAGE.items():
        if sol_key not in outputs:
            continue
        target = outputs[sol_key]
        if not target.exists():
            continue

        sol = load_pickle(target.path)

        # For routing_sol the value may be a list[RoutingSolution]
        if isinstance(sol, list):
            algo = sol[0].algo_name if sol else "unknown"
            time = sum(s.execution_time for s in sol)
        else:
            algo = getattr(sol, "algo_name", type(sol).__name__)
            time = getattr(sol, "execution_time", 0.0)

        result[stage_name] = {
            "stage": stage_name,
            "task_class": type(task).__name__,
            "algo": algo,
            "time": time,
            "solution": sol,
            "target_path": target.path,
        }
        break  # one entry per task
