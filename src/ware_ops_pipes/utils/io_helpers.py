import json
import pickle
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
