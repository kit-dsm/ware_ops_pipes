from __future__ import annotations

from ware_ops_pipes.pipelines.io_helpers import load_pickle


_SOL_KEY_TO_STAGE = {
    "item_assignment_sol": "item_assignment",
    "batching_sol": "batching",
    "routing_sol": "routing",
    "sequencing_sol": "scheduling",
    "assignment_sol": "assignment",
    "scheduling_sol": "scheduling",
}


def _iter_deps(task) -> list:
    deps = task.requires()

    if deps is None:
        return []

    if isinstance(deps, dict):
        return list(deps.values())

    if isinstance(deps, (list, tuple, set)):
        return list(deps)

    return [deps]


def _fingerprint_info(task) -> dict:
    config = (
        task.config_fingerprint_payload()
        if hasattr(task, "config_fingerprint_payload")
        else {}
    )

    return {
        "algo_fingerprint": (
            task.algo_fingerprint()
            if hasattr(task, "algo_fingerprint")
            else None
        ) or None,
        "own_fingerprint": (
            task.own_fingerprint()
            if hasattr(task, "own_fingerprint")
            else None
        ) or None,
        "chain_fingerprint": (
            task.chain_fingerprint()
            if hasattr(task, "chain_fingerprint")
            else None
        ) or None,
        "config": config or None,
    }


def collect_from_graph(task) -> dict[str, dict]:
    """Walk the task DAG depth-first and collect solutions + provenance.

    Returns a dict keyed by stage name, for example:

        {
            "item_assignment": {
                "stage": "item_assignment",
                "task_class": "GreedyIA",
                "algo": "GreedyItemAssignment",
                "time": 0.003,
                "solution": <ItemAssignmentSolution>,
                "target_path": ".../item_assignment_sol.pkl",
                "algo_fingerprint": "...",
                "own_fingerprint": "...",
                "chain_fingerprint": "...",
                "config": None,
            },
            "batching": { ... },
            "routing":  { ... },
        }

    Fingerprint semantics:

        algo_fingerprint:
            fingerprint of the base algorithm implementation

        own_fingerprint:
            fingerprint of the configured component itself

        chain_fingerprint:
            fingerprint of this component plus all upstream components
    """
    result: dict[str, dict] = {}
    _collect_recursive(task, result, visited=set())
    return result


def _collect_recursive(task, result: dict, visited: set) -> None:
    task_id = id(task)
    if task_id in visited:
        return

    visited.add(task_id)

    for dep in _iter_deps(task):
        _collect_recursive(dep, result, visited)

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
            **_fingerprint_info(task),
        }

        break