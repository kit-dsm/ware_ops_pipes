import json
import pickle
from pathlib import Path
from typing import Type, Dict, Any


def load_pickle(
        path: str,
        mode: str = "rb"
) -> Any:
    with open(path, mode) as f:
        return pickle.load(f)


def load_json(
        path: str,
        mode: str = "r"

) -> Dict:
    with open(path, mode) as f:
        return json.load(f)


def dump_json(
        path: str,
        data: Dict,
        encoder_cls: Type[json.JSONEncoder] | None = None,
        mode: str = "w",
        indent: int = 4

) -> None:
    with open(path, mode) as f:
        json.dump(data, f, cls=encoder_cls, indent=indent)


def dump_pickle(
        path: str,
        data: Any,
        mode: str = "wb"
) -> None:
    with open(path, mode) as f:
        pickle.dump(data, f)


def find_project_root() -> Path:
    """Find project root by looking for a marker file."""
    current = Path().resolve()
    for parent in [current] + list(current.parents):
        if (parent / "pyproject.toml").exists():  # or setup.py, .git, etc.
            return parent
    raise FileNotFoundError("Could not find project root")
