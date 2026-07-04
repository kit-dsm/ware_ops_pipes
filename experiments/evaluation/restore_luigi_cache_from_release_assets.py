from __future__ import annotations

import tarfile
from pathlib import Path


EVALUATION_DIR = Path(__file__).resolve().parent
EXPERIMENTS_DIR = EVALUATION_DIR.parent

OUTPUT_DIR = EXPERIMENTS_DIR / "output"
CACHE_IN_DIR = EVALUATION_DIR / "downloaded_cache"


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    archives = sorted(CACHE_IN_DIR.glob("luigi-cache-*.tar.gz"))
    if not archives:
        print(f"No cache archives found in {CACHE_IN_DIR}")
        return

    for archive in archives:
        print(f"Extracting {archive}")
        with tarfile.open(archive, "r:gz") as tar:
            tar.extractall(OUTPUT_DIR)

    print(f"Restored cache into {OUTPUT_DIR}")


if __name__ == "__main__":
    main()