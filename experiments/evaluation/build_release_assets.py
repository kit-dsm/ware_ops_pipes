from __future__ import annotations

import tarfile
from pathlib import Path


EVALUATION_DIR = Path(__file__).resolve().parent
EXPERIMENTS_DIR = EVALUATION_DIR.parent

OUTPUT_DIR = EXPERIMENTS_DIR / "output"
CACHE_DIR = EVALUATION_DIR / "release_cache"

SHARDS = {
    "loader": {
        "orders.pkl",
        "resources.pkl",
        "layout.pkl",
        "articles.pkl",
        "storage.pkl",
        "warehouse_info.pkl",
    },
    "item-assignment": {"item_assignment_sol.pkl"},
    "batching": {"batching_sol.pkl"},
    "routing": {"routing_sol.pkl"},
    "scheduling": {"scheduling_sol.pkl"},
    "summaries": {"summary.json"},
}


def collect_files(names: set[str]) -> list[Path]:
    return [
        path
        for path in OUTPUT_DIR.rglob("*")
        if path.is_file() and path.name in names
    ]


def make_tar_gz(shard_name: str, files: list[Path]) -> Path:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)

    out_path = CACHE_DIR / f"luigi-cache-{shard_name}.tar.gz"

    with tarfile.open(out_path, "w:gz") as tar:
        for path in files:
            arcname = path.relative_to(OUTPUT_DIR)
            tar.add(path, arcname=arcname)

    return out_path


def main() -> None:
    if not OUTPUT_DIR.exists():
        raise FileNotFoundError(f"Missing output directory: {OUTPUT_DIR}")

    for shard_name, filenames in SHARDS.items():
        files = collect_files(filenames)
        if not files:
            print(f"{shard_name}: no files")
            continue

        out_path = make_tar_gz(shard_name, files)
        size_mib = out_path.stat().st_size / 1024 / 1024
        print(f"{shard_name}: {len(files)} files -> {out_path} ({size_mib:.2f} MiB)")


if __name__ == "__main__":
    main()