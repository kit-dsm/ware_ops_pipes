from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any


class DataLoader(ABC):
    def __init__(self, data_dir: str | Path):
        self.data_dir = Path(data_dir)

    @abstractmethod
    def load(self, *args, **kwargs) -> Any:
        pass

    def _load_text(self, filename: str | Path, encoding: str = "utf-8") -> list[str]:
        path = Path(filename)
        if not path.is_absolute():
            path = self.data_dir / path
        with path.open("r", encoding=encoding) as file:
            return [line.strip() for line in file if line.strip()]
